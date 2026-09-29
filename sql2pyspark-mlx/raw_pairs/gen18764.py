from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy(F.month("log_ts")).orderBy(F.col("host_length").asc_nulls_first())
w3 = Window.partitionBy("host")
w4 = Window.partitionBy(F.year("log_ts"), "host").orderBy(F.col("host_length").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

filtered = (
    flattened
    .filter((F.col("log_ts") > F.to_date(F.lit("2025-06-01"))) & F.col("key").isNotNull())
    .select("*", F.length("host").alias("host_length"))
)

metrics = (
    filtered
    .select(
        "*",
        F.min("host_length").over(w).alias("key_min_host_length"),
        F.percent_rank().over(w2).alias("host_length_percent_rank"),
        F.max("host_length").over(w3).alias("host_max_host_length"),
        F.cume_dist().over(w4).alias("host_length_cume_dist"),
    )
)

base = (
    metrics
    .filter(F.col("key").isNotNull() & (F.col("log_ts") > F.lit("2025-06-01 08:00:00").cast("timestamp")))
    .withColumn("host_length_cume_dist_times_key_min_host_length", (F.col("host_length_cume_dist") * F.col("key_min_host_length")))
    .withColumn("log_ts_hour", F.hour("log_ts"))
    .withColumn("key_filled", F.coalesce("key", F.lit("NONE")))
)

result = (
    base
    .groupBy("service")
    .agg(
        F.max("log_ts").alias("last_log_ts"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_rows"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_count"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_rows"),
        F.sum("host_length_percent_rank").alias("total_host_length_percent_rank"),
    )
    .orderBy("h1_rows")
)
