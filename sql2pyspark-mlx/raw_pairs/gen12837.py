from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.length(F.col("c.name")) > 5)
    .select(F.col("a.id"), F.col("a.balance"), F.col("c.country"))
    .orderBy("country")
)
