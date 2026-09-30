from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("days_since_log_ts")
w2 = Window.partitionBy("host").orderBy("latest_rank", "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

valid = (
    lines
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "attr_key",
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked_app_logs = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

base = (
    ranked_app_logs
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .withColumn("host_service", F.concat("host", F.lit(" "), "service"))
)

result = (
    base
    .select(
        "id",
        "log_ts",
        "attr_key",
        "days_since_log_ts",
        "rn",
        F.when(F.col("attr_key").rlike("^(trace|region)$"), "GROUP_B").when(F.col("attr_key").rlike("^(version)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key_zone").desc(), F.col("days_since_log_ts").desc())
)
