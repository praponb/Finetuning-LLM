from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.city"))
    .agg(
        F.max(F.col("o.amount")).alias("max_amount"),
        (F.max(F.col("o.amount")) - F.min(F.col("o.amount"))).alias("amount_range"),
    )
    .orderBy("amount_range")
)
