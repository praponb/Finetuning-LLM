from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & F.col("monthly_fee").isNull() & (F.col("customer_id") == 27))
    .select("id", "monthly_fee")
    .orderBy(F.col("monthly_fee").desc())
)
