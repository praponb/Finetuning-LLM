from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("c.signup_date"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
    )
    .orderBy("account_type", "signup_date")
)
