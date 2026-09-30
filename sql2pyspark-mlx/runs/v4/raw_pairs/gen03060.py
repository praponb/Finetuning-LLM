from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("level")
w_ordered = Window.partitionBy("attr_key").orderBy(F.col("status_code_whole").desc_nulls_last())
w4 = Window.partitionBy("attr_key")
w5 = Window.partitionBy("service").orderBy("status_code_whole")
w6 = Window.partitionBy("level", "attr_key").orderBy(F.col("status_code_whole_max_15m").desc_nulls_last())
w7 = Window.partitionBy("level", "service")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

cleaned = (
    flattened
    .filter((F.col("log_ts") >= F.lit("2025-06-01 23:00:00").cast("timestamp")) & F.col("attr_key").isNotNull() & (F.col("attr_key") != "trace"))
    .select(
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        F.ceil("status_code").alias("status_code_whole"),
    )
)

scored = (
    cleaned
    .withColumn("status_code_whole_max_15m", F.max("status_code_whole").over(w))
    .withColumn("level_max_latency_ms", F.max("latency_ms").over(w2))
    .withColumn("status_code_whole_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4))
)

enriched = (
    scored
    .withColumn("status_code_whole_percent_rank", F.percent_rank().over(w5))
    .withColumn("status_code_whole_max_15m_dense_rank", F.dense_rank().over(w6))
    .withColumn("level_stddev_samp_status_code_whole", F.stddev_samp("status_code_whole").over(w7))
)

grouped = (
    enriched
    .groupBy(F.date_trunc("hour", "log_ts").alias("hour_window"))
    .agg(
        F.count(F.when(F.col("service") == "search", 1)).alias("search_rows"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
    )
)

result = (
    grouped
    .select("hour_window", "search_rows", "service_list", "latency_ms_bit_and")
    .orderBy(F.col("search_rows").desc_nulls_last())
)
