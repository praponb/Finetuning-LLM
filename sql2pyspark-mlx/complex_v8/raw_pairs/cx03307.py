from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality = Window.partitionBy("site_id", "quality")
w_quality_site_id_by_voltage = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage").asc_nulls_last(), "id")

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.abs(F.col("kwh") - 80.8).alias("kwh_distance"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.sum("kwh_distance").over(w_site_id_quality).alias("site_id_quality_sum_kwh_distance"),
        F.lag("kwh_distance").over(w_quality_site_id_by_voltage).alias("prev_kwh_distance"),
        (F.col("kwh_distance") - F.lag("kwh_distance").over(w_quality_site_id_by_voltage)).alias("kwh_distance_change"),
    )
)

grouped = (
    with_windows
    .filter(F.col("kwh_distance_change") >= -36.0)
    .groupBy("quality", F.to_date("read_at").alias("read_at_day"), "site_id")
    .agg(
        F.round(F.sum("prev_kwh_distance"), 2).alias("total_prev_kwh_distance"),
        F.max("kwh_distance_change").alias("max_kwh_distance_change"),
        F.min("voltage").alias("min_voltage"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .groupingSets(
        [["site_id", "quality", "distinct_ids"], ["site_id"], ["quality"], []],
        "site_id", "quality", "distinct_ids",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.max("read_at_day").alias("last_read_at_day"),
        F.datediff(F.max("read_at_day"), F.min("read_at_day")).alias("read_at_day_span_days"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
        F.col("distinct_ids").asc_nulls_last(),
    )
)
