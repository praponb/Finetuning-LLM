from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff_by_kwh_full = Window.partitionBy("site_tariff").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_tariff_by_kwh = Window.partitionBy("site_tariff").orderBy(F.col("kwh").desc(), "id")
w_site_id_site_zone_by_read_at = Window.partitionBy("site_id", "site_zone").orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_zone_voltage_tier = Window.partitionBy("site_zone", "voltage_tier")
w_site_tariff_by_kwh_prior_1 = Window.partitionBy("site_tariff").orderBy(F.col("kwh").desc(), "id").rowsBetween(-1, -1)
w_site_tariff_by_kwh_moving_6 = Window.partitionBy("site_tariff").orderBy(F.col("kwh").desc(), "id").rowsBetween(-6, Window.currentRow)
w_site_id_site_zone_by_read_at_2 = Window.partitionBy("site_id", "site_zone").orderBy(F.col("read_at").desc_nulls_first(), "id")
w_site_tariff_by_kwh_ties = Window.partitionBy("site_tariff").orderBy(F.col("kwh").desc())
w_site_zone_by_read_at_ties = Window.partitionBy("site_zone").orderBy("read_at")
w_site_commissioned_on_month_b = Window.partitionBy(F.month("site_commissioned_on")).orderBy(F.col("read_at").asc_nulls_last())
w_site_commissioned_on_year_vo = Window.partitionBy(F.year("site_commissioned_on"), "voltage_tier").orderBy(F.col("prev2_kwh").desc_nulls_first(), "id")
w_site_commissioned_on_year_vo_2 = Window.partitionBy(F.year("site_commissioned_on"), "voltage_tier").orderBy(F.col("prev2_kwh").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_site_commissioned_on_year_vo_3 = Window.partitionBy(F.year("site_commissioned_on"), "voltage_tier").orderBy(F.col("prev2_kwh").desc_nulls_first(), "id").rowsBetween(-3, -1)

base_readings = (
    df_meter_readings
    .filter(F.col("quality") != "EST")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 232.7, "HIGH").when(F.col("voltage") >= 219.6, "MEDIUM").otherwise("LOW").alias("voltage_tier"),
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
    )
)

joined_site = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-12-03").cast("date")))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.voltage_tier",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_windows = (
    joined_site
    .select(
        "*",
        F.first("site_commissioned_on").over(w_site_tariff_by_kwh_full).alias("first_site_commissioned_on"),
        F.ntile(4).over(w_site_tariff_by_kwh).alias("kwh_quartile"),
        F.last("site_commissioned_on", ignorenulls=True).over(w_site_id_site_zone_by_read_at).alias("last_known_site_commissioned_on"),
        F.size(F.collect_set("site_tariff").over(w_site_zone_voltage_tier)).alias("distinct_site_tariff_in_site_zone_voltage_tier"),
        F.round(F.avg("kwh").over(w_site_tariff_by_kwh_prior_1), 2).alias("kwh_avg_1_prior"),
        F.count(F.lit(1)).over(w_site_zone_voltage_tier).alias("rows_in_site_zone_voltage_tier"),
        F.max("kwh").over(w_site_tariff_by_kwh_moving_6).alias("kwh_max_7"),
        F.lag("kwh", 2).over(w_site_id_site_zone_by_read_at_2).alias("prev2_kwh"),
        F.dense_rank().over(w_site_tariff_by_kwh_ties).alias("kwh_dense_rank"),
        F.rank().over(w_site_zone_by_read_at_ties).alias("read_at_rank"),
        F.rank().over(w_site_commissioned_on_month_b).alias("read_at_rank_2"),
    )
)

annotated = with_windows.withColumn("read_at_rank_distance", F.abs(F.col("read_at_rank") - 4))

sequenced = (
    annotated
    .select(
        "*",
        F.ntile(4).over(w_site_commissioned_on_year_vo).alias("prev2_kwh_quartile"),
        F.row_number().over(w_site_commissioned_on_year_vo).alias("prev2_kwh_seq"),
        F.sum("read_at_rank_2").over(w_site_commissioned_on_year_vo_2).alias("read_at_rank_2_sum_4"),
        F.round(F.avg("read_at_rank_2").over(w_site_commissioned_on_year_vo_3), 2).alias("read_at_rank_2_avg_3_prior"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "voltage_tier",
        "site_commissioned_on",
        "site_zone",
        "site_tariff",
        "first_site_commissioned_on",
        "kwh_quartile",
        "last_known_site_commissioned_on",
        "distinct_site_tariff_in_site_zone_voltage_tier",
        "kwh_avg_1_prior",
        "rows_in_site_zone_voltage_tier",
        "kwh_max_7",
        "prev2_kwh",
        "kwh_dense_rank",
        "read_at_rank",
        "read_at_rank_2",
        "read_at_rank_distance",
        "prev2_kwh_seq",
        "read_at_rank_2_avg_3_prior",
        F.when(F.col("kwh_dense_rank") >= 7, "large").when(F.col("kwh_dense_rank") >= 4, "medium").otherwise("small").alias("kwh_dense_rank_tier"),
        F.concat(F.col("site_tariff"), F.lit("#"), F.col("kwh_quartile").cast("string")).alias("site_tariff_ref"),
    )
    .orderBy(F.col("rows_in_site_zone_voltage_tier").desc_nulls_first())
)
