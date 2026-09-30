from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .select(F.col("c.segment"), F.col("c.name"), F.col("c.email"), F.col("a.status"))
)
