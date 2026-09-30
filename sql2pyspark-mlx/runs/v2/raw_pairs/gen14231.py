from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .select(
        F.col("orders.order_date"),
        F.col("shipments.cost"),
        F.round((F.col("orders.amount") * 0.9), 2).alias("amount_adjusted"),
    )
    .orderBy(F.col("amount_adjusted").desc())
)
