from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("customers.country"))
    .agg(
        (F.max(F.col("accounts.balance")) - F.min(F.col("accounts.balance"))).alias("balance_range"),
    )
    .orderBy("country")
    .limit(5)
)
