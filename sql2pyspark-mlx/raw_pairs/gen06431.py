from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "host")
w2 = Window.partitionBy(F.month("log_ts"), "k")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

with_windows = (
    lines
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
    .withColumn("status_code_share_2", F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4))
)

base = (
    with_windows
    .filter((F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 14:00:00"))) & (F.col("status_code_share") > 0.23))
    .select(
        "*",
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_clean"),
    )
)

valid = (
    base
    .filter((F.col("log_ts") >= F.lit("2025-06-01 06:00:00").cast("timestamp")) & F.col("k").isNotNull())
    .select(
        "id",
        "log_ts",
        "status_code_share",
        "status_code_share_2",
        "days_since_log_ts",
        "k_clean",
        F.round("status_code_share", 2).alias("status_code_share_rounded"),
    )
)

result = (
    valid
    .rollup(
        F.when(F.col("status_code_share") >= 0.71, "PREMIUM").otherwise("STANDARD").alias("status_code_share_tier"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("status_code_share", 0.9).alias("status_code_share_p90"),
        F.min("log_ts").alias("first_log_ts"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.min("log_ts").alias("started_at"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
