from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("start_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("start_date"), "customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("plan").orderBy("start_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-3, -1)
w6 = Window.orderBy("monthly_fee_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.year("end_date")).orderBy("first_known_monthly_fee", "id")
w8 = Window.partitionBy(F.month("end_date")).orderBy("monthly_fee_sum_90d", "id")
w9 = Window.orderBy("first_known_monthly_fee", "id")
w10 = Window.orderBy("monthly_fee_avg_3_prior", "id")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.initcap(F.trim("plan")).alias("plan_title"))
)

base = (
    valid
    .filter((F.col("end_date") >= F.lit("2023-09-27").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("start_date_period", F.date_trunc("YEAR", "start_date"))
)

scored = (
    base
    .filter(F.year("end_date") == 2024)
    .select(
        "id",
        "end_date",
        F.sum("monthly_fee").over(w).alias("monthly_fee_sum_90d"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
        F.sum("monthly_fee").over(w3).alias("cumulative_monthly_fee"),
        F.first("monthly_fee", ignorenulls=True).over(w4).alias("first_known_monthly_fee"),
        F.avg("monthly_fee").over(w5).alias("monthly_fee_avg_3_prior"),
    )
)

base_subscriptions = (
    scored
    .filter(F.year("end_date") == 2023)
    .select(
        "id",
        "end_date",
        "monthly_fee_sum_90d",
        "running_monthly_fee",
        "cumulative_monthly_fee",
        "first_known_monthly_fee",
        "monthly_fee_avg_3_prior",
        (F.col("first_known_monthly_fee") + F.col("cumulative_monthly_fee")).alias("first_known_monthly_fee_plus_cumulative_monthly_fee"),
    )
)

enriched = (
    base_subscriptions
    .select(
        "id",
        "end_date",
        "monthly_fee_sum_90d",
        "first_known_monthly_fee",
        "monthly_fee_avg_3_prior",
        F.max("first_known_monthly_fee").over(w6).alias("running_max_first_known_monthly_fee"),
        F.min("first_known_monthly_fee").over(w6).alias("running_min_first_known_monthly_fee"),
        F.avg("first_known_monthly_fee").over(w6).alias("running_avg_first_known_monthly_fee"),
        F.ntile(10).over(w7).alias("first_known_monthly_fee_bucket"),
    )
)

filtered = (
    enriched
    .filter((F.col("end_date") > F.lit("2023-10-25").cast("date")) & (F.col("first_known_monthly_fee") <= 220))
    .withColumn("monthly_fee_avg_3_prior_num", F.col("monthly_fee_avg_3_prior").cast("double"))
    .withColumn("running_min_first_known_monthly_fee_int", F.col("running_min_first_known_monthly_fee").cast("int"))
)

scored_subscriptions = (
    filtered
    .withColumn("prev_monthly_fee_sum_90d", F.lag("monthly_fee_sum_90d", 1).over(w8))
    .withColumn("first_known_monthly_fee_bucket_2", F.ntile(4).over(w9))
    .withColumn("next_monthly_fee_avg_3_prior", F.lead("monthly_fee_avg_3_prior", 1).over(w10))
)

stats = (
    scored_subscriptions
    .withColumn("monthly_fee_avg_3_prior_tier", F.when(F.col("monthly_fee_avg_3_prior") >= 192, "PREMIUM").otherwise("STANDARD"))
    .cube("monthly_fee_avg_3_prior_tier")
    .agg(
        F.percentile_approx("running_min_first_known_monthly_fee_int", 0.5).alias("running_min_first_known_monthly_fee_int_p50"),
        F.percentile_approx("first_known_monthly_fee_bucket", [0.25, 0.5, 0.75]).alias("first_known_monthly_fee_bucket_quartiles"),
        F.round(F.avg("running_avg_first_known_monthly_fee"), 2).alias("avg_running_avg_first_known_monthly_fee"),
        F.round(F.avg("running_avg_first_known_monthly_fee"), 2).alias("avg_running_avg_first_known_monthly_fee_2"),
        F.grouping("monthly_fee_avg_3_prior_tier").alias("gid"),
    )
)

result = (
    stats
    .select(
        "monthly_fee_avg_3_prior_tier",
        "running_min_first_known_monthly_fee_int_p50",
        "first_known_monthly_fee_bucket_quartiles",
        "avg_running_avg_first_known_monthly_fee",
        "avg_running_avg_first_known_monthly_fee_2",
        "gid",
    )
)
