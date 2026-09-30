from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment").isin(["SMB", "Corporate", "Consumer"]) | F.col("c.country").isin(["JP", "SG", "UK"]))
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("c.name"),
        F.col("c.city"),
    )
    .orderBy(F.col("store_id").desc(), "id")
    .limit(10)
)
