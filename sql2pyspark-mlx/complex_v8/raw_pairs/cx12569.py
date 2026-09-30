from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality = Window.partitionBy("site_id", "quality")
w_quality_by_read_at_ties = Window.partitionBy("quality").orderBy(F.col("read_at").asc_nulls_last())
w_quality_by_read_at = Window.partitionBy("quality").orderBy(F.col("read_at").asc_nulls_last(), "id")

scoped = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage") < 234.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.abs(F.col("voltage") - 233.1).alias("voltage_distance"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id_quality), F.lit(0)), 4).alias("voltage_share_of_site_id_quality"),
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
        F.dense_rank().over(w_quality_by_read_at_ties).alias("read_at_dense_rank"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_quality_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.sum("voltage_distance").over(w_site_id_quality).alias("site_id_quality_sum_voltage_distance"),
    )
)

screened = (
    metrics
    .crossJoin(metrics.agg(F.min("read_at_dense_rank").alias("overall_min_read_at_dense_rank")))
    .filter(F.col("read_at_dense_rank") > F.col("overall_min_read_at_dense_rank"))
    .drop("overall_min_read_at_dense_rank")
)

result = (
    screened
    .cube("read_at_dense_rank", "site_id")
    .agg(
        F.round(F.sum("voltage_share_of_site_id_quality") / F.nullif(F.sum("rows_in_site_id_quality"), F.lit(0)), 3).alias("voltage_share_of_site_id_quality_to_rows_in_site"),
    )
    .orderBy(
        F.col("read_at_dense_rank").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
