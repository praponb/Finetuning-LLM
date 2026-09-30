from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.countDistinct("monthly_fee").alias("unique_statuss"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("customer_summary"), F.col("customers.id") == F.col("customer_summary.customer_id"), "left")
    .filter(F.col("customer_summary.min_monthly_fee") >= 210)
    .select(
        F.col("customers.credit_limit"),
        F.col("customers.age"),
        F.col("customer_summary.min_monthly_fee"),
        F.col("customer_summary.unique_statuss"),
    )
)
