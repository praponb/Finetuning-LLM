from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code")
w2 = Window.partitionBy(F.month("log_ts")).orderBy(F.col("is_flagged").desc_nulls_first())
w3 = Window.partitionBy(F.date_trunc("week", "log_ts")).orderBy(F.col("is_flagged_cume_dist").asc_nulls_first())
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.orderBy(F.col("status_code_cume_dist").desc_nulls_first())
w6 = Window.orderBy(F.col("status_code_high").desc_nulls_first())
w_grp = Window.orderBy(F.col("status_code_cume_dist_cume_dist").desc_nulls_last())
w8 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w9 = Window.orderBy(F.col("is_flagged").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "log_ts",
        "status_code",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    parsed
    .filter(F.to_date("log_ts") >= "2023-08-12")
    .withColumn("status_code_cume_dist", F.cume_dist().over(w))
    .withColumn("is_flagged_cume_dist", F.cume_dist().over(w2))
)

metrics_app_logs = (
    metrics
    .select(
        "id",
        "log_ts",
        "status_code",
        "is_flagged",
        "status_code_high",
        F.cume_dist().over(w3).alias("is_flagged_cume_dist_cume_dist"),
        F.avg("status_code").over(w4).alias("status_code_avg_30m"),
        F.cume_dist().over(w5).alias("status_code_cume_dist_cume_dist"),
    )
)

scored = (
    metrics_app_logs
    .withColumn("status_code_high_dense_rank", F.dense_rank().over(w6))
    .withColumn("status_code_cume_dist_cume_dist_rank", F.rank().over(w_grp))
)

scored_app_logs = (
    scored
    .select(
        "*",
        F.max("status_code").over(w8).alias("status_code_max_1h"),
        F.cume_dist().over(w).alias("status_code_cume_dist"),
        F.max("status_code").over(w4).alias("status_code_max_30m"),
        F.cume_dist().over(w9).alias("is_flagged_cume_dist"),
    )
)

result = (
    scored_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        "status_code_high",
        "status_code_cume_dist_cume_dist",
        "status_code_cume_dist_cume_dist_rank",
        "is_flagged_cume_dist",
    )
    .orderBy("is_flagged_cume_dist")
)
