from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.orderBy(F.col("adjusted_status_code").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

filtered = (
    lines
    .filter(F.col("status_code") <= 319)
    .select(
        "host",
        "log_ts",
        "status_code",
        "attr_value",
        F.round((F.col("status_code") * 100), 2).alias("adjusted_status_code"),
    )
)

metrics = (
    filtered
    .select(
        "host",
        "log_ts",
        "status_code",
        "attr_value",
        "adjusted_status_code",
        F.min("adjusted_status_code").over(w).alias("host_min_adjusted_status_code"),
        F.percent_rank().over(w2).alias("adjusted_status_code_percent_rank"),
        F.max("adjusted_status_code").over(w).alias("host_max_adjusted_status_code"),
    )
)

cleaned = (
    metrics
    .filter(F.col("adjusted_status_code_percent_rank").isNotNull())
    .withColumn("adjusted_status_code_percent_rank_whole", F.floor("adjusted_status_code_percent_rank"))
)

base = (
    cleaned
    .filter((F.col("log_ts") >= F.lit("2025-06-01 12:00:00").cast("timestamp")) & (F.col("host_max_adjusted_status_code") < 54))
    .select(
        "host",
        "status_code",
        "attr_value",
        "adjusted_status_code",
        "host_min_adjusted_status_code",
        "adjusted_status_code_percent_rank",
        "host_max_adjusted_status_code",
        F.initcap(F.trim("host")).alias("host_title"),
    )
)

result = (
    base
    .groupBy("host")
    .agg(
        F.round(F.avg("host_min_adjusted_status_code"), 2).alias("avg_host_min_adjusted_status_code"),
        F.percentile_approx("adjusted_status_code", [0.25, 0.5, 0.75]).alias("adjusted_status_code_quartiles"),
        F.count_if(F.col("host") == "h2").alias("h2_events"),
        F.sum(F.when(F.col("host") == "h1", F.col("adjusted_status_code"))).alias("h1_total_adjusted_status_code"),
        F.round(F.avg("host_min_adjusted_status_code"), 2).alias("avg_host_min_adjusted_status_code_2"),
        F.countDistinct("host").alias("distinct_hosts"),
    )
    .orderBy(F.col("distinct_hosts").asc_nulls_first())
)
