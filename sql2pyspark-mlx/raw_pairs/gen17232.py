from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "host").orderBy(F.col("years_since_log_ts").desc())
w2 = Window.partitionBy("service", "key").orderBy(F.col("years_since_log_ts").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode("context").alias("key", "value"),
    )
)

filtered = (
    exploded
    .filter(F.col("log_ts") >= F.to_date(F.lit("2025-06-01")))
    .select(
        "*",
        (F.datediff(F.current_date(), "log_ts") / 365).alias("years_since_log_ts"),
        F.when(F.col("service") == "search", 1).otherwise(0).alias("is_search"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_filled"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest = (
    deduped
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .select(
        "id",
        "service",
        "host",
        "key",
        "value",
        "years_since_log_ts",
        F.when(F.col("service").rlike("^(checkout|billing)$"), "PRIORITY").when(F.col("service").rlike("^(auth|search)$"), "CORE").otherwise("GROUP_B").alias("service_zone"),
    )
    .orderBy("key", "years_since_log_ts")
)
