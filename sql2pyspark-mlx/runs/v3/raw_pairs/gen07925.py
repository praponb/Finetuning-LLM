from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.segment"), F.col("c.is_active"))
    .agg(F.max(F.col("a.opened_date")).alias("latest_opened_date"))
    .orderBy(F.col("segment").desc())
)
