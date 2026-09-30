from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("a.status") == "CLOSED") & (F.col("a.opened_date") <= "2025-06-16"))
    .select(F.col("a.account_type"), F.col("c.email"), F.col("c.country"))
)
