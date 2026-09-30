from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy("paid_at", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("method").orderBy(F.col("status_length").desc(), "id")
w3 = Window.partitionBy("status", "method").orderBy(F.col("paid_at").desc_nulls_last(), "id")
w4 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("status")
w6 = Window.partitionBy("method").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.round((F.col("amount") * 100), 2).alias("amount_scaled"),
        F.length("status").alias("status_length"),
    )
)

with_windows = (
    filtered
    .withColumn("status_length_avg_5", F.avg("status_length").over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .filter(F.col("status_length") == 23)
    .select(
        "id",
        "method",
        "paid_at",
        "status_length",
        "status_length_avg_5",
        "seq_no",
        F.sum("status_length").over(w4).alias("status_length_sum_30d"),
        F.max("amount_scaled").over(w5).alias("status_max_amount_scaled"),
        F.sum("amount_scaled").over(w6).alias("cumulative_amount_scaled"),
    )
)

result = (
    enriched
    .select(
        "id",
        "method",
        "paid_at",
        "status_length_avg_5",
        "seq_no",
        "status_length_sum_30d",
        "cumulative_amount_scaled",
        F.when(F.col("method").rlike("^(bank_transfer|cash)$"), "GROUP_B").when(F.col("method").rlike("^(card|paypal)$"), "EXTENDED").otherwise("PRIORITY").alias("method_zone"),
    )
    .orderBy(F.col("status_length_avg_5").desc(), "seq_no")
)
