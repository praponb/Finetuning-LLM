from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .groupBy(F.col("c.is_active"))
    .agg(
        (F.max(F.col("o.discount")) - F.min(F.col("o.discount"))).alias("discount_range"),
        F.min(F.col("o.amount")).alias("min_amount"),
    )
    .filter(F.col("min_amount") >= 1220)
    .orderBy(F.col("min_amount").desc())
)
