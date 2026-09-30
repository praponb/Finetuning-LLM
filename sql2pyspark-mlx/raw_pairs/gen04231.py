from pyspark.sql import functions as F

amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

remaining_keys = (
    df_shipments
    .select("carrier", "order_id")
    .subtract(
        df_shipments
        .filter(F.col("cost") > 91)
        .select("carrier", "order_id")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.col("sum_payment_amount"),
    )
)
