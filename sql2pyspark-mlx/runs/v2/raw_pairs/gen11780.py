from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .filter(F.col("s.customer_id").isNotNull())
    .join(df_customers.filter(F.col("email").like("%@outlook.com")).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_anti")
    .select(F.col("s.id"), F.col("s.start_date"), F.col("s.plan"), F.col("s.status"))
)
