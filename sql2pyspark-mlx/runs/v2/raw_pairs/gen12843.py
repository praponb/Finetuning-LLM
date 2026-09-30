from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.city"))
    .agg(F.round(F.stddev(F.col("a.balance")), 2).alias("stddev_balance"))
)
