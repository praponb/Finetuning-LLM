from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_by_kwh_prior_6 = Window.partitionBy("site_zone").orderBy("kwh", "id").rowsBetween(-6, -1)
w_all_by_voltage_running = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_zone_by_kwh_ties = Window.partitionBy("site_zone").orderBy("kwh")
w_site_zone_by_kwh_moving_3 = Window.partitionBy("site_zone").orderBy("kwh", "id").rowsBetween(-3, Window.currentRow)
w_site_zone = Window.partitionBy("site_zone")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_site_id = Window.partitionBy("site_id")
w_site_zone_by_kwh_running = Window.partitionBy("site_zone").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_zone_by_kwh = Window.partitionBy("site_zone").orderBy("kwh", "id")
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy("voltage")

recent_readings = (
    df_meter_readings
    .filter(F.col("voltage") <= 234.7)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

enriched_site = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone").isNotNull())
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

with_trends = (
    enriched_site
    .select(
        "*",
        F.sum("kwh").over(w_site_zone_by_kwh_prior_6).alias("kwh_sum_6_prior"),
        F.min("kwh").over(w_all_by_voltage_running).alias("min_kwh_so_far"),
        F.rank().over(w_site_zone_by_kwh_ties).alias("kwh_rank"),
        F.round(F.avg("voltage").over(w_site_zone_by_kwh_moving_3), 2).alias("voltage_avg_4"),
        F.size(F.collect_set("site_id").over(w_site_zone)).alias("distinct_site_id_in_site_zone"),
        (F.col("kwh") - F.lag("kwh").over(w_all_by_voltage)).alias("kwh_change"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.first("site_commissioned_on", ignorenulls=True).over(w_site_zone_by_kwh_running).alias("first_known_site_commissioned_on"),
        F.dense_rank().over(w_site_zone_by_kwh_ties).alias("kwh_dense_rank"),
        F.lag("read_at").over(w_site_zone_by_kwh).alias("prev_read_at"),
        F.sum("kwh").over(w_site_id).alias("site_id_sum_kwh"),
        F.row_number().over(w_all_by_voltage).alias("rn"),
        F.dense_rank().over(w_site_id_by_voltage_ties).alias("voltage_dense_rank"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
    )
)

derived = (
    with_trends
    .filter(F.col("first_known_site_commissioned_on") < F.lit("2024-11-21").cast("date"))
    .select(
        "*",
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("site_zone")), " ", "_")).alias("site_zone_slug"),
    )
)

result = (
    derived
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "site_zone",
        "site_commissioned_on",
        "kwh_sum_6_prior",
        "min_kwh_so_far",
        "kwh_rank",
        "voltage_avg_4",
        "distinct_site_id_in_site_zone",
        "kwh_change",
        "rows_in_site_id",
        "first_known_site_commissioned_on",
        "kwh_dense_rank",
        "site_id_sum_kwh",
        "rn",
        "voltage_dense_rank",
        "voltage_vs_site_id_avg",
        "read_at_hour",
        "site_zone_slug",
        F.round(F.col("voltage_vs_site_id_avg") * 1.1, 2).alias("voltage_vs_site_id_avg_adj"),
    )
    .orderBy(F.col("site_id_sum_kwh").desc(), F.col("rows_in_site_id").desc())
)
