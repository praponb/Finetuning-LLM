from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").asc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status", "method").orderBy(F.col("paid_at").desc_nulls_first(), "id")

valid = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-05-05 14:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("clean_method", F.trim("method"))
)

base = (
    valid
    .filter(F.col("paid_at") > F.lit("2023-11-03 09:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "amount_int",
        F.coalesce("method", F.lit("Unknown")).alias("method_filled"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

prev_events = latest.withColumn("prev_paid_at", F.lag("paid_at").over(w2))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w3))

deduped = (
    islands
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "method_filled",
        "resolved_method",
        "dedup_rank",
        "prev_paid_at",
        "session_no",
        F.when(F.col("status").rlike("^(FAILED|REFUNDED)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("status_zone"),
    )
)
