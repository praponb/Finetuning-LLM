from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(F.upper(F.trim(F.col("c.email"))) == "ALICE@GMAIL.COM")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("c.age"),
        F.col("c.is_active"),
        F.col("c.segment"),
        F.trim(F.col("c.name")).alias("clean_name"),
    )
    .orderBy(F.col("age").desc())
)
