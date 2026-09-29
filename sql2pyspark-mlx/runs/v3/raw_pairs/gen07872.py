from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(F.col("c.email").isNotNull() & ~F.col("c.email").like("%@gmail.com"))
    .select(F.col("s.start_date"), F.col("c.name"), F.col("c.age"))
    .orderBy("start_date")
)
