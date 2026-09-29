from pyspark.sql import functions as F

amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

enriched = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.shipped_date",
        "a.cost",
        "a.status",
        F.col("d.amount"),
    )
)

result = (
    enriched
    .join(amount_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "shipped_date", "cost", F.col("min_payment_amount"))
    .orderBy(F.col("order_id").asc_nulls_first())
)
