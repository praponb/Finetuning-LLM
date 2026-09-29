from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .filter(F.col("s.customer_id").isNotNull())
    .join(df_customers.filter(F.col("email").isNotNull()).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("s.start_date") > "2023-09-01")
    .select(F.col("s.id"), F.col("s.plan"))
)
