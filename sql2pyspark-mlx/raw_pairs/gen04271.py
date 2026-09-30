from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("retry_count", "id")
w2 = Window.partitionBy("host").orderBy(F.col("retry_count").desc(), "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context")["trace"].alias("trace"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.col("context")["region"].alias("region"),
    )
)

result = (
    unpacked
    .filter(F.col("retry_count") != 1)
    .select(
        "id",
        "log_ts",
        "region_or_default",
        "region",
        F.lead("retry_count", 1).over(w).alias("next_retry_count"),
        F.ntile(5).over(w2).alias("retry_count_bucket"),
    )
)
