from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy(F.month("log_ts")).orderBy(F.col("status_code_max_15m").asc_nulls_first())
w3 = Window.partitionBy("key", "host")
w4 = Window.partitionBy("level", "key")
w5 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.orderBy(F.col("status_code_max_15m_rank").desc())
w7 = Window.partitionBy("key")
w8 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

enriched = (
    lines
    .filter(F.col("log_ts") >= "2023-04-01")
    .select("*", F.max("status_code").over(w).alias("status_code_max_15m"))
)

with_windows = enriched.select("*", F.rank().over(w2).alias("status_code_max_15m_rank"))

metrics = (
    with_windows
    .withColumn("status_code_max_15m_share", F.round((F.col("status_code_max_15m") / F.sum("status_code_max_15m").over(w3)), 4))
    .withColumn("level_avg_status_code_max_15m", F.avg("status_code_max_15m").over(w4))
    .withColumn("status_code_max_15m_rank_avg_1h", F.avg("status_code_max_15m_rank").over(w5))
    .withColumn("status_code_max_15m_rank_cume_dist", F.cume_dist().over(w6))
)

enriched_app_logs = (
    metrics
    .select(
        "*",
        F.avg("status_code_max_15m_rank").over(w7).alias("key_avg_status_code_max_15m_rank"),
        F.sum("status_code_max_15m_rank").over(w8).alias("status_code_max_15m_rank_sum_2h"),
    )
)

result = (
    enriched_app_logs
    .select(
        "host",
        "log_ts",
        "status_code_max_15m_share",
        "level_avg_status_code_max_15m",
        "status_code_max_15m_rank_avg_1h",
        "status_code_max_15m_rank_sum_2h",
        F.when(F.col("host").rlike("^(h3|h4)$"), "EXTENDED").otherwise("PRIORITY").alias("host_zone"),
    )
    .orderBy(F.col("status_code_max_15m_rank_avg_1h").desc_nulls_first())
)
