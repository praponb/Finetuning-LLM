from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.carrier").isin(["UPS", "FedEx", "Kerry"]) & F.col("s.order_id").isin([14, 17, 23, 24]))
    .select(
        F.col("s.cost"),
        F.col("o.store_id"),
        F.col("c.is_active"),
        F.col("c.city"),
        F.when(F.col("s.carrier") == "Flash", 1).otherwise(0).alias("is_flash"),
    )
)
