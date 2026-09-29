from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan").isin(["standard", "premium"]) & (F.col("end_date") > "2025-10-01")) | (F.col("end_date") >= "2025-07-28"))
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy(F.col("status").desc())
)
