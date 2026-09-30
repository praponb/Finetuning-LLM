from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.amount").between(530, 1400) & (F.col("o.status") == "RETURNED"))
    .select(F.col("o.store_id"), F.col("c.country"))
    .orderBy("country", F.col("store_id").desc())
)
