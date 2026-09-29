from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(F.col("a.opened_date") >= "2024-06-01")
    .groupBy(
        F.col("c.is_active"),
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(F.avg(F.col("a.balance")).alias("avg_balance"))
    .filter(F.col("avg_balance") > 22000)
    .orderBy("signup_month", "is_active")
    .limit(5)
)
