from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc_nulls_last(), "id")

base = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-10-07").cast("date")) & F.month("start_date").isin([1, 6, 9]))
    .select("*", F.lower("status").alias("status_lower"))
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_dims = (
    latest
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.plan",
        "a.start_date",
        "a.monthly_fee",
        F.col("b.age"),
        F.col("b.name"),
        F.col("b.city"),
    )
)

attributed = (
    with_dims
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.start_date") >= F.col("sub.start_date")) & (F.col("a.start_date") <= F.date_add(F.col("sub.end_date"), 3)), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

totals = (
    attributed
    .withColumn("activity_month", F.date_trunc("MONTH", "start_date"))
    .groupingSets(
        [["activity_month", "subscription_plan"], ["activity_month"]],
        "activity_month",
        "subscription_plan",
    )
    .agg(
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.avg("age").alias("avg_age"),
        F.min("subscription_id").alias("min_subscription_id"),
        F.size(F.collect_set("subscription_plan")).alias("num_subscription_plans"),
        F.percentile_approx("subscription_monthly_fee", [0.25, 0.5, 0.75]).alias("subscription_monthly_fee_quartiles"),
        F.grouping("activity_month").alias("grouping_level"),
    )
    .filter(F.col("num_subscription_plans") > 1)
)

result = (
    totals
    .select(
        "activity_month",
        "subscription_plan",
        "num_plans",
        "avg_age",
        "min_subscription_id",
        "num_subscription_plans",
        "subscription_monthly_fee_quartiles",
        "grouping_level",
    )
    .orderBy("grouping_level")
)
