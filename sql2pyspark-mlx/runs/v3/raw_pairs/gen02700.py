from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.status").isin(["COMPLETED", "CANCELLED", "PENDING"]))
    .select(
        F.col("s.carrier"),
        F.col("s.shipped_date"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("c.segment"),
        F.col("c.age"),
    )
    .orderBy(F.col("segment").desc())
)
