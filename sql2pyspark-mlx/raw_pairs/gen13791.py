from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.orderBy(F.col("discount").asc_nulls_first())
w7 = Window.partitionBy("status").orderBy(F.col("order_date").asc_nulls_first(), "id")

flagged = (
    df_payments
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

enriched = (
    islands
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.amount",
        "e.paid_at",
        "e.status",
        "e.is_new_session",
        F.col("o.discount"),
        F.col("o.order_date"),
    )
)

filtered = (
    enriched
    .filter(F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .select(
        "*",
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.ceil("discount").alias("discount_whole"),
    )
)

enriched_payments = (
    filtered
    .filter(F.col("status").isin(["FAILED", "REFUNDED"]))
    .select(
        "id",
        "paid_at",
        "status",
        "is_new_session",
        "discount",
        "order_date",
        "paid_at_day",
        "discount_whole",
        F.sum("discount_whole").over(w3).alias("discount_whole_sum_90d"),
        F.max("discount_whole").over(w4).alias("discount_whole_max_90d"),
        F.sum("discount_whole").over(w5).alias("discount_whole_sum_90d_2"),
        F.dense_rank().over(w6).alias("discount_dense_rank"),
    )
)

deduped = (
    enriched_payments
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("discount_whole_max_90d"))).alias("refunded_total_discount_whole_max_90d"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.percentile_approx("discount_whole_max_90d", 0.95).alias("discount_whole_max_90d_p95"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
        F.min("paid_at").alias("first_paid_at"),
        F.percentile_approx("discount_whole_sum_90d_2", [0.25, 0.5, 0.75]).alias("discount_whole_sum_90d_2_quartiles"),
    )
    .orderBy(F.col("success_rows").desc_nulls_first())
)
