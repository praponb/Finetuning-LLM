from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "customer_id")
    .agg(
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
        F.min("start_date").alias("first_start_date"),
    )
    .orderBy(F.col("customer_id").desc(), F.col("basic_monthly_fee").desc(), "plan")
    .limit(5)
)
