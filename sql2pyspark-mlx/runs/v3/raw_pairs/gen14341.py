from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.customer_id").isNull() & F.col("s.status").isin(["CANCELLED", "EXPIRED"]))
    .select(F.col("s.id"), F.col("s.start_date"), F.col("c.country"))
    .orderBy(F.col("start_date").desc())
)
