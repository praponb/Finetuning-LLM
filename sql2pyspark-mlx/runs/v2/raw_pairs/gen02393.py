from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .groupBy(F.col("c.country"), F.col("c.is_active"))
    .agg(
        F.round(F.stddev(F.col("a.balance")), 2).alias("stddev_balance"),
        F.sort_array(F.collect_set(F.col("a.status"))).alias("status_list"),
    )
)
