from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(140, 190) | F.col("customer_id").isNotNull())
    .select("id", "monthly_fee")
)
