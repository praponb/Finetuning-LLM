from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("flag_bit4", "id")
w2 = Window.orderBy("retry_count", "id").rowsBetween(-2, -1)
w3 = Window.orderBy(F.col("flag_bit4").desc(), "id")
w4 = Window.orderBy("status_code", "id")

flattened_attrs = (
    df_app_logs
    .filter(F.col("status_code") < 272)
    .select(
        "id",
        "status_code",
        "payload_json",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.avg("flag_bit4").over(w2).alias("flag_bit4_avg_2_prior"),
        F.row_number().over(w3).alias("seq_no"),
        F.lead("retry_count", 1).over(w4).alias("next_retry_count"),
    )
)
