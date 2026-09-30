from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.year(F.col("c.signup_date")).alias("signup_year"),
    )
    .agg(
        F.sum(F.when(F.col("a.account_type") == "SAVINGS", 1).otherwise(0)).alias("savings_count"),
    )
    .orderBy("signup_month", "signup_year")
    .limit(5)
)
