from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("c.country") == "DE")
    .select(F.col("a.customer_id"), F.col("c.name"))
    .orderBy(F.col("customer_id").desc())
)
