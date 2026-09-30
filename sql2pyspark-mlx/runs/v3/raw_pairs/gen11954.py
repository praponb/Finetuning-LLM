from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.store_id").isin([25, 32]) & (F.col("c.credit_limit") > 5200))
    .select(
        F.col("o.order_date"),
        F.col("c.city"),
        (F.col("o.amount") / F.nullif(F.col("o.discount"), F.lit(0))).alias("amount_per_discount"),
    )
)
