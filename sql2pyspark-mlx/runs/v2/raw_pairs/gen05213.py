from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "outer")
    .filter(F.length(F.col("c.name")) < 12)
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.cost"),
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("c.country"),
        F.col("c.email"),
        F.trim(F.col("s.carrier")).alias("clean_carrier"),
    )
    .orderBy(F.col("carrier").desc())
)
