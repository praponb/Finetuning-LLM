from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(~F.col("city").isin(["Bangkok", "Singapore"])).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.customer_id").isin([32, 35]))
    .select(F.col("s.id"), F.col("s.monthly_fee"))
    .orderBy("id")
    .limit(3)
)
