from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("key").orderBy("status_code", "id", "key")
w2 = Window.partitionBy("key").orderBy("flag_bit8", "id", "key")
w3 = Window.partitionBy("key").orderBy("next_status_code_num", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key")
w_ordered = Window.partitionBy("key").orderBy(F.col("prev_status_code").desc_nulls_last())
w6 = Window.partitionBy("key").orderBy(F.col("seq_no").desc(), "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

unpacked = (
    flattened
    .select(
        "id",
        "status_code",
        "key",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    unpacked
    .select(
        "id",
        "key",
        "status_code_high",
        F.lead("status_code").over(w_hist).alias("next_status_code"),
        F.lag("status_code").over(w_hist).alias("prev_status_code"),
        F.lag("status_code_high").over(w_hist).alias("prev_status_code_high"),
        F.lead("status_code_high").over(w_hist).alias("next_status_code_high"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.lag("status_code", 2).over(w2).alias("prev_status_code_2"),
    )
)

valid = (
    enriched
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.col("next_status_code").cast("double").alias("next_status_code_num"),
        F.coalesce("key", F.lit("NONE")).alias("resolved_key"),
    )
)

enriched_app_logs = (
    valid
    .select(
        "*",
        F.last("key", ignorenulls=True).over(w3).alias("last_known_key"),
        F.min("status_code_high").over(w4).alias("key_min_status_code_high"),
        F.percent_rank().over(w_ordered).alias("prev_status_code_percent_rank"),
    )
)

deduped = (
    enriched_app_logs
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "key",
        "status_code_high",
        "next_status_code",
        "prev_status_code",
        "prev_status_code_high",
        "next_status_code_high",
        "seq_no",
        "next_status_code_num",
    )
    .orderBy(F.col("status_code_high").desc())
)
