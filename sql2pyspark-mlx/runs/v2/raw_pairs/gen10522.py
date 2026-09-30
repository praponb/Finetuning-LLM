from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["basic", "premium"]) & F.col("customer_id").isNotNull())
    .select("id", "start_date", "monthly_fee")
    .orderBy(F.col("monthly_fee").desc())
)
