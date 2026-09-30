from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.opened_date") >= "2024-08-26")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.sum(F.col("a.balance")).alias("total_balance"),
        F.max(F.col("a.opened_date")).alias("latest_opened_date"),
    )
    .orderBy(F.col("signup_month").desc())
)
