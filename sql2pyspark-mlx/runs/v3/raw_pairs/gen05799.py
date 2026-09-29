from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.max(F.col("a.opened_date")).alias("latest_opened_date"),
        F.count(F.col("a.balance")).alias("balance_count"),
    )
)
