from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("status").orderBy("paid_at", "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status", "method").orderBy("island_id", "id")
w5 = Window.partitionBy(F.month("paid_at"), "method").orderBy("session_start_flag", "id")
w6 = Window.orderBy("event_seq")
w7 = Window.partitionBy("status", "method").orderBy("next_session_start_flag", "id")
w8 = Window.partitionBy("status").orderBy("session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("status").orderBy(F.col("next_amount").desc(), "id")

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-08-24 00:00:00"))) & F.col("status").isin(["SUCCESS", "REFUNDED"]))
    .withColumn("method_lower", F.lower("method"))
    .withColumn("method_clean", F.coalesce("method", F.lit("NONE")))
)

deduped = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "method", "amount", "paid_at", "status", "method_lower")
)

flagged = (
    deduped
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w2).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w2) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("session_start_flag").over(w3))

metrics = (
    islands
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        "method_lower",
        "session_start_flag",
        F.lead("session_start_flag").over(w4).alias("next_session_start_flag"),
        F.lag("session_start_flag").over(w4).alias("prev_session_start_flag"),
        F.lead("amount").over(w4).alias("next_amount"),
        F.lag("amount").over(w4).alias("prev_amount"),
        F.row_number().over(w4).alias("seq_no"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w6).alias("event_seq_cume_dist"),
        F.ntile(10).over(w7).alias("next_session_start_flag_bucket"),
        F.sum("next_amount").over(w8).alias("cumulative_next_amount"),
        F.ntile(5).over(w9).alias("next_amount_bucket"),
    )
)

result = (
    enriched
    .select(
        "id",
        "method",
        "amount",
        "status",
        "method_lower",
        "session_start_flag",
        "next_session_start_flag",
        "prev_session_start_flag",
        "next_amount",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
