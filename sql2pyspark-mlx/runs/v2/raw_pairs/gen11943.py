from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "left")
    .groupBy(
        F.year(F.col("customers.signup_date")).alias("signup_year"),
        F.col("customers.city"),
    )
    .agg(
        F.min(F.col("accounts.balance")).alias("min_balance"),
        F.count("*").alias("account_count"),
    )
)
