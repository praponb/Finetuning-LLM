from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.account_type").isin(["CHECKING", "CREDIT"]))
    .groupBy(F.col("c.city"))
    .agg(
        F.count(F.col("a.balance")).alias("balance_count"),
        F.round(F.stddev(F.col("a.balance")), 2).alias("stddev_balance"),
    )
    .orderBy(F.col("balance_count").desc())
)
