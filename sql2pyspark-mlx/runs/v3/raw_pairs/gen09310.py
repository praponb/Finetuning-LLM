from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.country"))
    .agg(F.min(F.col("a.opened_date")).alias("min_opened_date"))
)
