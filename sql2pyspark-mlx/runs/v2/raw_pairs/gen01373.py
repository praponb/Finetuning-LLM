from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("c.segment").isin(["SMB", "Corporate"]) & F.col("a.customer_id").isin([7, 8, 10, 31]))
    .select(F.col("a.account_type"), F.col("c.signup_date"))
)
