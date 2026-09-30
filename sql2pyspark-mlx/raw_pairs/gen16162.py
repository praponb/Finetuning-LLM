from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("dim"), F.col("a.customer_id") == F.col("dim.id"), "left")
    .groupBy(
        F.col("dim.city").alias("city"),
        F.col("a.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("a.balance")), 2).alias("total_balance"),
    )
)
