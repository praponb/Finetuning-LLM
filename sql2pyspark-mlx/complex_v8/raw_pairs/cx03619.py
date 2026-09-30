from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality = Window.partitionBy("site_id", "quality")
w_all_by_site_commissioned_on = Window.orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(-2, -1)
w_site_id_site_tariff_by_kwh_t = Window.partitionBy("site_id", "site_tariff").orderBy("kwh")
w_site_tariff_by_kwh_moving_1 = Window.partitionBy("site_tariff").orderBy(F.col("kwh").desc(), "id").rowsBetween(-1, Window.currentRow)
w_quality_site_tariff_by_kwh_r = Window.partitionBy("quality", "site_tariff").orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id = Window.partitionBy("site_id")
w_quality_site_tariff_by_kwh_p = Window.partitionBy("quality", "site_tariff").orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_quality_by_kwh_filled = Window.partitionBy("quality").orderBy("kwh_filled", "id")
w_site_id_site_commissioned_on = Window.partitionBy("site_id", F.year("site_commissioned_on"))
w_all_by_kwh_percent_rank = Window.orderBy("kwh_percent_rank", "id")
w_quality_site_tariff = Window.partitionBy("quality", "site_tariff")
w_all_by_kwh_percent_rank_movi = Window.orderBy("kwh_percent_rank", "id").rowsBetween(-1, Window.currentRow)
w_all_by_kwh_percent_rank_runn = Window.orderBy("kwh_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_readings = (
    df_meter_readings
    .filter(F.col("kwh") <= 90.6)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

with_details = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
        F.round(F.avg("voltage").over(w_all_by_site_commissioned_on), 2).alias("voltage_avg_2_prior"),
        F.size(F.collect_set("site_tariff").over(w_site_id_quality)).alias("distinct_site_tariff_in_site_id_quality"),
        F.round(F.percent_rank().over(w_site_id_site_tariff_by_kwh_t), 3).alias("kwh_percent_rank"),
        F.sum("kwh").over(w_site_tariff_by_kwh_moving_1).alias("kwh_sum_2"),
        F.first("voltage", ignorenulls=True).over(w_quality_site_tariff_by_kwh_r).alias("first_known_voltage"),
        F.size(F.collect_set("site_tariff").over(w_site_id)).alias("distinct_site_tariff_in_site_id"),
        F.max("kwh_filled").over(w_quality_site_tariff_by_kwh_p).alias("kwh_filled_max_1_prior"),
        F.row_number().over(w_quality_by_kwh_filled).alias("kwh_filled_seq"),
        F.round(F.avg("kwh").over(w_site_id_quality), 2).alias("site_id_quality_avg_kwh"),
        F.round(F.avg("voltage").over(w_site_id), 2).alias("site_id_avg_voltage"),
        F.max("voltage").over(w_site_id_site_commissioned_on).alias("site_id_site_commissioned_on_year_max_voltage"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.lead("kwh_filled_seq", 2, 0).over(w_all_by_kwh_percent_rank).alias("next2_kwh_filled_seq"),
        F.round(F.avg("first_known_voltage").over(w_quality_site_tariff), 2).alias("quality_site_tariff_avg_first_known_voltage"),
        F.sum("kwh_filled_max_1_prior").over(w_all_by_kwh_percent_rank_movi).alias("kwh_filled_max_1_prior_sum_2"),
        F.last("kwh", ignorenulls=True).over(w_all_by_kwh_percent_rank_runn).alias("last_known_kwh"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "kwh_filled",
        "site_name",
        "site_commissioned_on",
        "site_tariff",
        "voltage_avg_2_prior",
        "distinct_site_tariff_in_site_id_quality",
        "first_known_voltage",
        "kwh_filled_max_1_prior",
        "site_id_quality_avg_kwh",
        "site_id_avg_voltage",
        "site_id_site_commissioned_on_year_max_voltage",
        "next2_kwh_filled_seq",
        "quality_site_tariff_avg_first_known_voltage",
        "kwh_filled_max_1_prior_sum_2",
        "last_known_kwh",
        F.lower(F.regexp_replace(F.trim(F.col("site_tariff")), " ", "_")).alias("site_tariff_slug"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("site_commissioned_on")).alias("days_since_site_commissioned_on"),
    )
    .orderBy(F.col("site_id_quality_avg_kwh").desc(), F.col("site_tariff").asc())
)
