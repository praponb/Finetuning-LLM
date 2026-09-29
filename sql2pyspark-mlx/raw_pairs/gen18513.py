from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at", "id")
w4 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method").orderBy(F.col("paid_at").asc_nulls_last(), "id")
w6 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w7 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("session_no", "id").rowsBetween(-6, -1)
w9 = Window.partitionBy("order_id", "method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("order_id", "method").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

flagged = (
    sessionized
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w3).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized_payments = (
    flagged
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.sum("is_new_burst").over(w4).alias("session_id"),
    )
)

deduped = (
    sessionized_payments
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

with_gaps_2 = (
    deduped
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w6).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w6) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized_payments_2 = with_gaps_2.withColumn("session_no", F.sum("is_new_burst").over(w7))

enriched = (
    sessionized_payments_2
    .select(
        "*",
        F.avg("is_new_burst").over(w8).alias("is_new_burst_avg_6_prior"),
        F.sum("session_no").over(w9).alias("cumulative_session_no"),
        F.last("method", ignorenulls=True).over(w10).alias("last_known_method"),
    )
)

result = (
    enriched
    .select(
        "id",
        "method",
        "paid_at",
        "session_id",
        "dedup_rank",
        "is_new_burst_avg_6_prior",
        "cumulative_session_no",
        "last_known_method",
        F.when(F.col("method").rlike("^(bank_transfer|promptpay)$"), "GROUP_B").when(F.col("method").rlike("^(card|paypal)$"), "PRIORITY").otherwise("EXTENDED").alias("method_zone"),
    )
    .orderBy(F.col("is_new_burst_avg_6_prior").desc_nulls_first())
)
