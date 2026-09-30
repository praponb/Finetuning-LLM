from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.is_active"))
    .agg(F.round(F.avg(F.col("a.balance")), 2).alias("avg_balance"))
)
