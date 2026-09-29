from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "basic"]))
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .filter(F.col("monthly_fee_count") < 3)
)
