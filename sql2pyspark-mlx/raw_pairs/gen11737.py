from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.month("end_date"), "subscription_plan")
w3 = Window.partitionBy(F.month("end_date"), "status").orderBy("subscription_id", "id", "subscription_id").rowsBetween(-2, -1)
w4 = Window.partitionBy("status").orderBy("start_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("subscription_plan", "plan").orderBy("start_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

attributed = (
    ranked
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

with_windows = (
    attributed
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4))
    .withColumn("monthly_fee_avg_2_prior", F.avg("monthly_fee").over(w3))
    .withColumn("first_known_subscription_monthly_fee", F.first("subscription_monthly_fee", ignorenulls=True).over(w4))
)

scored = (
    with_windows
    .select(
        "*",
        F.max("monthly_fee_avg_2_prior").over(w5).alias("running_max_monthly_fee_avg_2_prior"),
        F.count("monthly_fee_avg_2_prior").over(w5).alias("running_count_monthly_fee_avg_2_prior"),
        F.avg("monthly_fee_avg_2_prior").over(w5).alias("running_avg_monthly_fee_avg_2_prior"),
    )
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        "rank_in_group",
        "subscription_id",
        "subscription_plan",
        F.when(F.col("plan").rlike("^(enterprise|premium)$"), "GROUP_A").when(F.col("plan").rlike("^(basic|standard)$"), "PRIORITY").otherwise("GROUP_B").alias("plan_zone"),
    )
    .orderBy(F.col("rank_in_group").asc_nulls_first())
)
