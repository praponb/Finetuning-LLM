from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date_by_read_at_f = Window.partitionBy(F.to_date("read_at")).orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_adj_2_running = Window.orderBy(F.col("kwh_adj_2").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_adj_2 = Window.orderBy(F.col("kwh_adj_2").asc_nulls_last(), "id")
w_site_id_quality_by_kwh_prior = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(-6, -1)

valid_readings = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(10, 15))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 1.1, 2).alias("kwh_adj"),
        F.round(F.col("kwh") * 1.5, 2).alias("kwh_adj_2"),
    )
)

enriched = (
    valid_readings
    .filter(F.col("read_at") < F.lit("2025-02-24 11:00:00").cast("timestamp"))
    .select(
        "*",
        (F.col("kwh") + F.col("voltage")).alias("kwh_plus_voltage"),
        F.round(F.col("kwh_adj") / F.nullif(F.col("kwh_adj_2"), F.lit(0)), 3).alias("kwh_adj_per_kwh_adj_2"),
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.nth_value("kwh_adj_per_kwh_adj_2", 2).over(w_read_at_to_date_by_read_at_f).alias("second_kwh_adj_per_kwh_adj_2"),
        F.max("kwh").over(w_all_by_kwh_adj_2_running).alias("max_kwh_so_far"),
        F.lead("kwh_adj_per_kwh_adj_2", 3).over(w_all_by_kwh_adj_2).alias("next3_kwh_adj_per_kwh_adj_2"),
        F.sum("kwh_adj_per_kwh_adj_2").over(w_site_id_quality_by_kwh_prior).alias("kwh_adj_per_kwh_adj_2_sum_6_prior"),
        F.row_number().over(w_all_by_kwh_adj_2).alias("rn"),
    )
)

first_rows = (
    ranked
    .filter(F.col("rn") <= 3)
    .drop("rn")
)

result = (
    first_rows
    .groupBy("site_id", "read_at")
    .pivot("quality", ["OK", "BAD"])
    .agg(F.avg("kwh").alias("avg_v"), F.sum("kwh").alias("sum_v"))
    .orderBy("site_id", "read_at")
)
