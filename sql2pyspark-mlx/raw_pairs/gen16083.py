from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("retry_count", "id")
w2 = Window.partitionBy("service", "level").orderBy(F.col("retry_count").desc(), "id")

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "payload_json",
        "context",
        F.col("context").getItem("trace").alias("trace"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("direct")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    parsed
    .filter(F.col("host").isin(["h3", "h2"]))
    .withColumn("retry_count_bucket", F.ntile(4).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)
