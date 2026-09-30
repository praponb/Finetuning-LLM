from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("tier").orderBy("flag_bit1", "id")
w2 = Window.partitionBy("tier").orderBy(F.col("status_code").desc(), "id")

parsed = (
    df_app_logs
    .filter(F.col("latency_ms") <= 860)
    .select(
        "id",
        "latency_ms",
        "status_code",
        "payload_json",
        F.col("context")["region"].alias("region"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.lag("status_code").over(w_recent).alias("prev_status_code"),
        F.lead("status_code").over(w_recent).alias("next_status_code"),
        F.lead("flag_bit1").over(w_recent).alias("next_flag_bit1"),
        F.lag("flag_bit1").over(w_recent).alias("prev_flag_bit1"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
