from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("level").orderBy("num_context")
w2 = Window.partitionBy("level").orderBy("num_context", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("level").orderBy("duration_sec", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("level").orderBy(F.col("flag_bit8").desc_nulls_last())

unpacked = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.dense_rank().over(w_recent).alias("num_context_dense_rank"),
        F.sum("flag_bit8").over(w2).alias("cumulative_flag_bit8"),
        F.avg("flag_bit8").over(w_part).alias("flag_bit8_avg_5"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4).alias("latency_ms_share"),
        F.percent_rank().over(w5).alias("flag_bit8_percent_rank"),
    )
)
