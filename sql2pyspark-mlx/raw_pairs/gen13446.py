from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("service").orderBy(F.col("days_since_log_ts").desc_nulls_last())
w3 = Window.orderBy(F.col("days_since_log_ts").desc_nulls_first())
w4 = Window.partitionBy(F.month("log_ts"), "level").orderBy(F.col("days_since_log_ts").asc_nulls_last())
w_hist = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "v"),
    )
)

cleaned = (
    flattened
    .filter((F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 14:00:00"))) & (F.col("log_ts") < "2024-06-01"))
    .select(
        "service",
        "log_ts",
        "level",
        "key",
        "v",
        F.substring("service", 1, 4).alias("service_prefix"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .withColumn("days_since_log_ts", F.datediff(F.current_date(), "log_ts"))
    .withColumn("level_group", F.when(F.col("level").isin(["ERROR", "DEBUG"]), "Priority").otherwise("Other"))
)

scored = (
    cleaned_app_logs
    .select(
        "log_ts",
        "v",
        "service_prefix",
        F.max("days_since_log_ts").over(w).alias("service_max_days_since_log_ts"),
        F.percent_rank().over(w2).alias("days_since_log_ts_percent_rank"),
        F.cume_dist().over(w3).alias("days_since_log_ts_cume_dist"),
        F.cume_dist().over(w4).alias("days_since_log_ts_cume_dist_2"),
    )
)

base = (
    scored
    .filter((F.col("log_ts") > F.lit("2025-06-01 10:00:00").cast("timestamp")) & (F.to_date("log_ts") >= "2023-04-01"))
    .select("*", F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"))
)

filtered = (
    base
    .filter((F.col("log_ts") >= F.lit("2025-06-01 11:00:00").cast("timestamp")) & (F.col("days_since_log_ts_percent_rank") >= 0.32))
    .withColumn("days_since_log_ts_cume_dist_2_per_days_since_log_ts_percent_rank", (F.col("days_since_log_ts_cume_dist_2") / F.nullif("days_since_log_ts_percent_rank", F.lit(0))))
)

result = (
    filtered
    .filter(F.col("days_since_log_ts") < 48)
    .select(
        "log_ts",
        "v",
        "days_since_log_ts_cume_dist",
        "days_since_log_ts",
        F.avg("days_since_log_ts_cume_dist_2").over(w_hist).alias("days_since_log_ts_cume_dist_2_avg_1h"),
    )
)
