from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.segment"))
    .agg(F.min(F.col("a.opened_date")).alias("first_opened_date"))
)
