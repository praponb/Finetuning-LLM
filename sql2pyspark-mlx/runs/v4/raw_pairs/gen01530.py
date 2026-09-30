from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(F.countDistinct(F.col("o.discount")).alias("unique_channels"))
    .filter(F.col("unique_channels") >= 3)
    .orderBy(F.col("signup_month").desc())
)
