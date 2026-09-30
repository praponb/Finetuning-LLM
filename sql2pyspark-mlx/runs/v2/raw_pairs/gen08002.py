from pyspark.sql import functions as F

valid_subscriptions = df_subscriptions.filter(~F.col("plan").isin(["enterprise", "premium", "basic"]))

customer_id_summary = (
    valid_subscriptions
    .groupBy("customer_id", "status")
    .agg(
        F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0)).alias("standard_count"),
    )
)

result = (
    customer_id_summary
    .filter(F.col("standard_count") >= 49)
    .select("customer_id", "status", "standard_count")
    .orderBy(F.col("standard_count").desc(), "status", "customer_id")
    .limit(5)
)
