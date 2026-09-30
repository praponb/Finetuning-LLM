from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.account_type").isin(["CREDIT", "SAVINGS"]))
    .select(F.col("a.status"), F.col("a.customer_id"), F.col("c.is_active"))
)
