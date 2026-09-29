from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.amount") >= 650)
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.cost"),
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("c.email"),
    )
    .orderBy(F.col("customer_id").desc())
)
