from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(~(~F.col("o.status").isin(["PENDING", "RETURNED", "COMPLETED"]) | F.col("o.channel").isNotNull()))
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("s.status"),
        F.col("o.store_id"),
        F.col("c.segment"),
    )
)
