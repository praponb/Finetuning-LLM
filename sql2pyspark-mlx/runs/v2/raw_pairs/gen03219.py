from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.status").isin(["CANCELLED", "RETURNED", "COMPLETED"]) & F.col("o.discount").between(0.33, 0.39))
    .select(F.col("s.cost"), F.col("o.order_date"), F.col("o.customer_id"))
    .orderBy(F.col("customer_id").desc())
)
