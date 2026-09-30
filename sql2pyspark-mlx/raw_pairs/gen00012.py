from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 3) & F.col("customer_id").isNotNull() & (F.col("status") == "EXPIRED"))
    .groupBy(F.month("end_date").alias("end_month"), "status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .orderBy(F.col("avg_monthly_fee").desc(), "end_month")
)
