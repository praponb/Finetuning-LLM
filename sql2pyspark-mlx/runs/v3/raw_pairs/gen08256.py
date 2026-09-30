from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("o.status") == "COMPLETED") & (F.col("o.store_id") == 7))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.store_id"),
        F.col("c.country"),
        F.col("c.credit_limit"),
        F.col("c.is_active"),
    )
)
