from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method").orderBy(F.col("amount").asc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc_nulls_last(), "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_ordered = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

with_windows = (
    df_payments
    .withColumn("last_known_method", F.last("method", ignorenulls=True).over(w))
    .withColumn("amount_dense_rank", F.dense_rank().over(w2))
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.status").alias("order_status"), F.col("d.discount"))
)

kept_keys = (
    enriched
    .select("method", "order_id")
    .exceptAll(
        df_payments
        .filter(F.to_date("paid_at") >= "2024-04-01")
        .select("method", "order_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

remaining_keys = (
    kept_rows
    .select("order_id", "status")
    .subtract(
        df_payments
        .filter(F.col("method").isin(["promptpay", "paypal"]))
        .select("order_id", "status")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .withColumn("discount_sum_30d", F.sum("discount").over(w4))
    .withColumn("amount_count_90d", F.count("amount").over(w_ordered))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("amount_count_90d") >= 14, "HIGH").otherwise("LOW").alias("amount_count_90d_tier"),
    )
    .agg(
        F.round(F.sum("discount_sum_30d") / F.nullif(F.count(F.when(F.col("method") == "cash", 1)), F.lit(0)), 2).alias("discount_sum_30d_per_cash"),
        F.avg(F.when(F.col("order_status") == "COMPLETED", F.col("discount"))).alias("completed_avg_discount"),
        F.min("paid_at").alias("started_at"),
        F.size(F.collect_set("order_status")).alias("num_order_status"),
    )
    .orderBy(F.col("amount_count_90d_tier").desc())
)
