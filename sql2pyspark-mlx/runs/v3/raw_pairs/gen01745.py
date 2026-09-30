from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("c.segment") == "SMB")
    .select(
        F.col("c.email"),
        F.col("a.opened_date"),
        F.date_trunc("week", F.col("c.signup_date")).alias("signup_date_period"),
    )
)
