from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.city") == "Bangkok")
    .groupBy(
        F.col("c.segment"),
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
    )
    .agg(
        F.avg(F.col("a.balance")).alias("avg_balance"),
        F.count(F.when(F.col("a.account_type") == "LOAN", 1)).alias("loan_count"),
    )
    .orderBy(F.col("segment").desc(), "signup_quarter")
    .limit(50)
)
