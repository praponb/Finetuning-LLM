from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("status_code", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("service").orderBy("status_code_high", "id")

parsed = (
    df_app_logs
    .filter(F.col("service").isin(["auth", "billing", "checkout"]))
    .select(
        "id",
        "service",
        "status_code",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    parsed
    .select(
        "id",
        "status_code",
        "duration_text",
        F.count("status_code").over(w).alias("last7_count_status_code"),
        F.sum("status_code").over(w).alias("last7_sum_status_code"),
        F.avg("status_code").over(w).alias("last7_avg_status_code"),
        F.max("status_code").over(w).alias("last7_max_status_code"),
        F.lead("status_code", 2, 0).over(w2).alias("next_status_code"),
    )
)
