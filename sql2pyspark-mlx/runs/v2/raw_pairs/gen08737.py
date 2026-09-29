from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.is_active"), F.col("c.country"))
    .agg(
        F.round(F.stddev(F.col("a.balance")), 2).alias("stddev_balance"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 1)
    .orderBy(F.col("stddev_balance").desc(), "row_count")
)
