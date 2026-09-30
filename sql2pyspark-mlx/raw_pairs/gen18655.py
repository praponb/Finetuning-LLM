from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("duration_sec", "id")
w2 = Window.partitionBy("host").orderBy("duration_sec", "id")

unpacked = (
    df_app_logs
    .filter(F.col("log_ts") < "2023-06-01")
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    unpacked
    .withColumn("duration_sec_bucket", F.ntile(4).over(w))
    .withColumn("next_duration_sec", F.lead("duration_sec", 2).over(w2))
)
