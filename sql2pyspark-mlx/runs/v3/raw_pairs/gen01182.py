from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.city"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("o.status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
    .orderBy("city", "pct_cancelled")
    .limit(1)
)
