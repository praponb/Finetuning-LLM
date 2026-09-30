from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.month("start_date").alias("start_month"), "status")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("monthly_fee_range") > 36)
    .orderBy("status", "start_month")
    .limit(5)
)
