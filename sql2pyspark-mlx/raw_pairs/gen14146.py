from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tier").orderBy("has_fault", "id")
w2 = Window.partitionBy("tier")

flattened_attrs = (
    df_app_logs
    .filter(F.to_date("log_ts") >= "2025-06-20")
    .select(
        "id",
        "log_ts",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "user_id",
        "duration_text",
        "tier",
        F.lead("has_fault").over(w).alias("next_has_fault"),
        F.row_number().over(w).alias("seq_no"),
        F.sum("has_fault").over(w2).alias("tier_sum_has_fault"),
    )
)
