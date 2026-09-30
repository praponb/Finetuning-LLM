from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.customer_id").isNull())
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.min(F.col("a.opened_date")).alias("min_opened_date"),
        F.round(F.stddev(F.col("a.balance")), 2).alias("stddev_balance"),
    )
    .orderBy(F.col("min_opened_date").desc())
)
