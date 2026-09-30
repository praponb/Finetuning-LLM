from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .filter(F.col("stores.opened_date").between("2023-05-01", "2023-07-22") & (F.col("payments.status") != "SUCCESS"))
    .select(
        F.col("payments.id"),
        F.col("payments.amount"),
        F.col("payments.order_id"),
        F.col("orders.status"),
        F.col("orders.shipping_fee"),
        F.col("stores.city"),
        F.col("stores.sqft"),
    )
    .orderBy(F.col("amount").desc(), F.col("order_id").desc())
)
