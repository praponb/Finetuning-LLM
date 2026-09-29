from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.date_trunc("QUARTER", "start_date").alias("quarter_start"),
        "plan",
        "customer_id",
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("monthly_fee", 0.5).alias("monthly_fee_p50"),
    )
)
