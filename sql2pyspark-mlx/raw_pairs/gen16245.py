from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy(F.col("paid_at").desc_nulls_first(), "id")
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (~F.col("method").eqNullSafe(F.lit("paypal"))))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.substring("status", 1, 3).alias("status_prefix"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

gaps = (
    deduped
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w3) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        F.sum("session_start_flag").over(w4).alias("island_id"),
    )
)

result = (
    sessionized
    .select("id", "status", "island_id")
    .orderBy(F.col("status").desc_nulls_last())
)
