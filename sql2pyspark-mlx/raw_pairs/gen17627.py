from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("retry_count", "id").rowsBetween(-5, -1)
w2 = Window.orderBy("duration_sec", "id")
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("level").orderBy(F.col("num_context").desc(), "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "level",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    unpacked
    .withColumn("duration_sec_avg_5_prior", F.avg("duration_sec").over(w))
    .withColumn("next_num_context", F.lead("num_context", 1).over(w2))
    .withColumn("level_stddev_samp_duration_sec", F.stddev_samp("duration_sec").over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("level_avg_duration_sec", F.avg("duration_sec").over(w3))
)
