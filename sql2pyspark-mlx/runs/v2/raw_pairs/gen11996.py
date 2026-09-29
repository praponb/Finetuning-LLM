from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("c.segment"),
        F.col("c.city"),
        F.col("c.credit_limit"),
        F.when(F.col("o.channel") == "web", 1).otherwise(0).alias("is_web"),
    )
)
