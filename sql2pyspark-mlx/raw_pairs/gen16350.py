from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("log_ts"))
w2 = Window.orderBy(F.col("status_code_high").asc_nulls_last())
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.orderBy(F.col("status_code_share").desc())

lines = (
    df_app_logs
    .select("id", "log_ts", "status_code", F.explode_outer("context").alias("key", "v"))
)

unpacked = (
    lines
    .filter(F.col("log_ts") < "2023-06-23")
    .select(
        "log_ts",
        "status_code",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
    )
)

metrics = (
    unpacked
    .select(
        "log_ts",
        "status_code",
        "v",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.dense_rank().over(w2).alias("status_code_high_dense_rank"),
    )
)

metrics_app_logs = (
    metrics
    .withColumn("status_code_high_dense_rank_sum_1h", F.sum("status_code_high_dense_rank").over(w3))
    .withColumn("status_code_share_dense_rank", F.dense_rank().over(w4))
)

result = (
    metrics_app_logs
    .groupBy(
        F.when(F.col("status_code_high_dense_rank_sum_1h") >= 33, "HIGH").otherwise("LOW").alias("status_code_high_dense_rank_sum_1h_tier"),
    )
    .agg(
        F.max("log_ts").alias("last_seen"),
        F.round(F.avg("status_code_high_dense_rank"), 2).alias("avg_status_code_high_dense_rank"),
        F.percentile_approx("status_code_share_dense_rank", 0.9).alias("status_code_share_dense_rank_p90"),
    )
    .filter(F.col("avg_status_code_high_dense_rank") > 0)
)
