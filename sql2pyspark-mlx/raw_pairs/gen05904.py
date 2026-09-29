from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("paid_at"), "order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("status", "method").orderBy(F.col("amount").asc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("month", "paid_at"), "status")
w5 = Window.partitionBy("status").orderBy("amount_share", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("last7_max_amount_max_7d", "id")
w7 = Window.partitionBy(F.to_date("paid_at"), "status")
w8 = Window.partitionBy("method").orderBy("amount_cume_dist", "id")

enriched = (
    df_payments
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.count("amount").over(w).alias("amount_count_90d"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
        F.max("amount").over(w3).alias("amount_max_7d"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
    )
)

metrics = (
    enriched
    .filter(F.col("amount_cume_dist") >= 0.28)
    .withColumn("last7_min_amount_max_7d", F.min("amount_max_7d").over(w5))
    .withColumn("last7_count_amount_max_7d", F.count("amount_max_7d").over(w5))
    .withColumn("last7_max_amount_max_7d", F.max("amount_max_7d").over(w5))
)

scored = (
    metrics
    .withColumn("next_amount_max_7d", F.lead("amount_max_7d").over(w6))
    .withColumn("prev_amount_max_7d", F.lag("amount_max_7d").over(w6))
    .withColumn("prev_amount_cume_dist", F.lag("amount_cume_dist").over(w6))
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("last7_count_amount_max_7d_share", F.round((F.col("last7_count_amount_max_7d") / F.sum("last7_count_amount_max_7d").over(w7)), 4))
)

with_windows = (
    scored
    .select(
        "id",
        "method",
        "status",
        "amount_count_90d",
        "amount_cume_dist",
        "amount_max_7d",
        "amount_share",
        "last7_count_amount_max_7d",
        "last7_max_amount_max_7d",
        "next_amount_max_7d",
        "prev_amount_max_7d",
        "seq_no",
        "last7_count_amount_max_7d_share",
        F.lag("prev_amount_cume_dist").over(w8).alias("prev_prev_amount_cume_dist"),
        F.lag("last7_max_amount_max_7d").over(w8).alias("prev_last7_max_amount_max_7d"),
        F.lead("last7_max_amount_max_7d").over(w8).alias("next_last7_max_amount_max_7d"),
        F.row_number().over(w8).alias("event_seq"),
    )
)

pivoted = (
    with_windows
    .groupBy("method")
    .pivot("status", ["REFUNDED", "FAILED"])
    .agg(F.avg("amount_cume_dist"))
    .select(
        "method",
        F.col("REFUNDED").alias("refunded"),
        F.col("FAILED").alias("failed"),
    )
)

result = (
    pivoted
    .select(
        "method",
        "failed",
        F.when(F.col("method").rlike("^(bank_transfer|promptpay)$"), "CORE").when(F.col("method").rlike("^(card|paypal)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("method_zone"),
    )
)
