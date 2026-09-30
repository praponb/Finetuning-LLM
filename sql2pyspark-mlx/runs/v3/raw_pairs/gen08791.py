from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") != "premium")
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"), "customer_id")
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .filter(F.col("enterprise_count") > 1)
)
