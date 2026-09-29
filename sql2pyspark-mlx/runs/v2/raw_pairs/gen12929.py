from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .filter((F.year(F.col("shipments.delivered_date")) == 2024) & (F.col("shipments.status") == "RETURNED"))
    .select(
        F.col("shipments.id"),
        F.col("shipments.carrier"),
        F.col("orders.status"),
        F.col("orders.amount"),
        F.col("orders.store_id"),
    )
    .orderBy(F.col("amount").desc(), "carrier")
)
