from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("a.balance").between(24000, 46000))
    .groupBy(F.col("c.is_active"))
    .agg(F.min(F.col("a.opened_date")).alias("min_opened_date"))
)
