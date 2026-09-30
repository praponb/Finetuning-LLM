from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.when(F.year(F.col("c.signup_date")) < 2024, "before_2024").when(F.year(F.col("c.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        (F.max(F.col("a.balance")) - F.min(F.col("a.balance"))).alias("balance_range"),
        F.sum(F.when(F.col("a.account_type") == "SAVINGS", F.col("a.balance")).otherwise(0)).alias("savings_balance"),
    )
)
