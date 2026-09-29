from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("c.signup_date"),
        F.round(F.col("c.age"), 2).alias("age_rounded"),
    )
)
