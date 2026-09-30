from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("a.account_type") == "CHECKING", 1).otherwise(0)).alias("checking_count"),
    )
)
