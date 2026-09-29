from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(~F.col("email").like("%@gmail.com")).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.status") != "EXPIRED")
    .select(F.col("s.id"), F.col("s.end_date"), F.col("s.plan"), F.col("s.monthly_fee"))
)
