from pyspark.sql import functions as F

not_flagged = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id").isin([3, 14, 33, 40])), "left_semi")
)

result = (
    not_flagged
    .groupBy("store_id")
    .agg(
        F.corr("shipping_fee", "amount").alias("corr_shipping_fee_amount"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("shipping_fee")).otherwise(0)).alias("pending_shipping_fee"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
    )
    .filter(F.col("cancelled_events") > 1)
    .orderBy(F.col("cancelled_events").desc_nulls_first())
)
