from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "right")
    .groupBy(
        F.date_format(F.col("customers.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(
        F.countDistinct(F.col("accounts.balance")).alias("unique_statuss"),
        (F.max(F.col("accounts.balance")) - F.min(F.col("accounts.balance"))).alias("balance_range"),
    )
    .orderBy("balance_range", "signup_month")
)
