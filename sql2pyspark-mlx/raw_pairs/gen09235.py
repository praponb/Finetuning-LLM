from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("status_code", "id").rowsBetween(-6, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "status_code",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context").getItem("version").alias("version"),
    )
)

result = (
    unpacked
    .filter(F.col("duration_sec") > 1.7)
    .select(
        "id",
        "status_code",
        "version",
        F.max("status_code").over(w_recent).alias("last7_max_status_code"),
        F.count("status_code").over(w_recent).alias("last7_count_status_code"),
        F.sum("status_code").over(w_recent).alias("last7_sum_status_code"),
    )
)
