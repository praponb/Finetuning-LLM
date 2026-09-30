from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("o.amount").isNotNull())
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"), F.col("c.city"))
    .agg(F.avg(F.col("o.discount")).alias("avg_discount"))
    .filter(F.col("avg_discount") >= 0.18)
)
