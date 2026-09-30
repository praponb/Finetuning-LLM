from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.country"))
    .agg(F.countDistinct(F.col("o.discount")).alias("unique_customers"))
    .orderBy(F.col("unique_customers").desc(), "country")
    .limit(3)
)
