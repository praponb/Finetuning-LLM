from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.col("city").isin(["Chiang Mai", "London", "New York"])).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.status").isin(["EXPIRED", "CANCELLED"]))
    .select(F.col("s.id"), F.col("s.monthly_fee"))
    .orderBy(F.col("id").desc())
)
