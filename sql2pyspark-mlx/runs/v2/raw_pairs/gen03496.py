from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "outer")
    .filter(F.col("s.sqft") == 6224)
    .groupBy(F.col("c.is_active"))
    .agg(
        F.countDistinct(F.col("o.amount")).alias("unique_stores"),
        F.round(F.avg(F.col("o.discount")), 2).alias("avg_discount"),
    )
)
