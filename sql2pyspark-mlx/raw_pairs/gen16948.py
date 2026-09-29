from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .filter((F.col("s.carrier") == "DHL") & F.col("o.status").isin(["SHIPPED", "COMPLETED", "PENDING"]))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.order_id"),
        F.col("o.customer_id"),
    )
    .orderBy(F.col("customer_id").desc())
)
