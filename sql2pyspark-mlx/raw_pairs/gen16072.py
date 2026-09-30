from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_semi")
    .filter(F.col("c.city") == "Chiang Mai")
    .select(F.col("c.id"), F.col("c.segment"))
)
