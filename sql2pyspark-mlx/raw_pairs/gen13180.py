from pyspark.sql import functions as F

quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

remaining_keys = (
    df_payments
    .select("status")
    .exceptAll(
        df_payments
        .filter(F.col("amount").between(1210, 1360))
        .select("status")
    )
)

verified = (
    df_payments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.col("sum_order_item_quantity"),
    )
)
