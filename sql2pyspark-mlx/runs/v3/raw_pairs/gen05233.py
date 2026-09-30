from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("o.store_id").isin([37, 38, 40]))
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("c.country"),
        F.col("c.is_active"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("amount").desc(), "id")
    .limit(20)
)
