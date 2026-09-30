from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(F.countDistinct(F.col("o.amount")).alias("unique_stores"))
    .orderBy("segment", F.col("unique_stores").desc())
    .limit(5)
)
