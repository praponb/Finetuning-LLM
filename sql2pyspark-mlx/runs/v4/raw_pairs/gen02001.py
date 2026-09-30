from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("customer_id", "plan").orderBy("end_date", "id")
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("start_date", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("customer_id")
w_hist = Window.partitionBy("plan", "customer_id")
w8 = Window.partitionBy("plan").orderBy("prev_monthly_fee", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(-5, -1)

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("status").isin(["CANCELLED", "PAUSED"]), "Core").otherwise("Other").alias("status_group"),
    )
)

base_subscriptions = (
    base
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
        F.concat("status", F.lit(", "), "plan").alias("status_plan"),
    )
)

base_subscriptions_2 = (
    base_subscriptions
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-08-02"))) & (F.col("days_since_start_date") < 20))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "status_group",
        "days_since_start_date",
        "status_plan",
        F.date_format("end_date", "yyyy-MM").alias("end_date_label"),
    )
)

with_windows = (
    base_subscriptions_2
    .withColumn("prev_monthly_fee", F.lag("monthly_fee", 1).over(w))
    .withColumn("next_days_since_start_date", F.lead("days_since_start_date", 1).over(w2))
)

metrics = (
    with_windows
    .withColumn("status_stddev_samp_days_since_start_date", F.stddev_samp("days_since_start_date").over(w3))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w4))
    .withColumn("monthly_fee_avg_5_prior", F.avg("monthly_fee").over(w5))
    .withColumn("prev_monthly_fee_share", F.round((F.col("prev_monthly_fee") / F.sum("prev_monthly_fee").over(w6)), 4))
)

with_windows_subscriptions = (
    metrics
    .select(
        "*",
        F.avg("next_days_since_start_date").over(w3).alias("status_avg_next_days_since_start_date"),
        F.avg("prev_monthly_fee_share").over(w_hist).alias("plan_avg_prev_monthly_fee_share"),
        F.avg("status_stddev_samp_days_since_start_date").over(w8).alias("status_stddev_samp_days_since_start_date_avg_5"),
        F.avg("prev_monthly_fee").over(w3).alias("status_avg_prev_monthly_fee"),
        F.avg("monthly_fee_avg_5_prior").over(w9).alias("monthly_fee_avg_5_prior_avg_5_prior"),
    )
)

result = (
    with_windows_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        "status",
        "days_since_start_date",
        "status_plan",
        "end_date_label",
        "next_days_since_start_date",
        "status_stddev_samp_days_since_start_date",
    )
    .orderBy(
        F.col("id").desc(),
        F.col("status_stddev_samp_days_since_start_date").desc(),
    )
)
