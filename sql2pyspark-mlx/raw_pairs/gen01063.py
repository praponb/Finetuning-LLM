from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("c.credit_limit"),
        F.col("c.name"),
        F.when(F.col("o.amount") >= 1760, "High").when(F.col("o.amount") >= 570, "Medium").otherwise("Low").alias("amount_tier"),
    )
    .orderBy(F.col("region").desc())
)
