from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("duration_sec", "id")
w_recent = Window.partitionBy("host").orderBy("is_flagged", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("service").orderBy("is_flagged", "id")
w4 = Window.partitionBy("service").orderBy("retry_count", "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.row_number().over(w_ordered).alias("row_seq"),
        F.sum("duration_sec").over(w_recent).alias("cumulative_duration_sec"),
        F.row_number().over(w_running).alias("seq_no"),
        F.sum("is_flagged").over(w_recent).alias("cumulative_is_flagged"),
        F.lag("status_code_high", 1, 0).over(w4).alias("prev_status_code_high"),
    )
)
