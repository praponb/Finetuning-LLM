from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.email").like("%@gmail.com") | F.col("o.customer_id").isin([2, 4, 12]))
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("c.segment"),
        F.coalesce(F.col("c.segment"), F.lit("N/A")).alias("segment_filled"),
    )
    .orderBy("segment")
)
