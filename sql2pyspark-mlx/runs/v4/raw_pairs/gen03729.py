from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("service").orderBy("duration_sec", "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.ntile(10).over(w).alias("duration_sec_bucket"),
        F.lag("duration_sec", 2, 0).over(w2).alias("prev_duration_sec"),
        F.ntile(4).over(w).alias("duration_sec_bucket_2"),
    )
)
