from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(-5, -1)
w_ordered = Window.partitionBy("customer_id").orderBy(F.col("running_monthly_fee").desc())
w5 = Window.orderBy("running_monthly_fee_cume_dist", "id")
w6 = Window.partitionBy(F.date_trunc("WEEK", "start_date")).orderBy("running_monthly_fee_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-08-08").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.date_format("start_date", "MMM yyyy").alias("start_date_label"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "plan_title",
        F.sum("monthly_fee").over(w_hist).alias("running_monthly_fee"),
        F.ntile(4).over(w2).alias("monthly_fee_bucket"),
    )
)

enriched_subscriptions = (
    enriched
    .filter(F.year("end_date") >= 2025)
    .withColumn("monthly_fee_avg_5_prior", F.avg("monthly_fee").over(w3))
    .withColumn("running_monthly_fee_cume_dist", F.cume_dist().over(w_ordered))
)

valid = (
    enriched_subscriptions
    .filter(F.col("start_date") > F.lit("2023-08-17").cast("date"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "plan_title",
        "monthly_fee_bucket",
        "monthly_fee_avg_5_prior",
        "running_monthly_fee_cume_dist",
        F.date_add("start_date", 44).alias("start_date_plus_44d"),
    )
)

scored = (
    valid
    .withColumn("next_monthly_fee_avg_5_prior", F.lead("monthly_fee_avg_5_prior", 2).over(w5))
    .withColumn("monthly_fee_avg_5_prior_avg_5_prior", F.avg("monthly_fee_avg_5_prior").over(w3))
    .withColumn("last_known_monthly_fee_bucket", F.last("monthly_fee_bucket", ignorenulls=True).over(w6))
)

totals = (
    scored
    .groupBy(
        "customer_id",
        F.date_trunc("MONTH", "end_date").alias("activity_month"),
        F.date_trunc("QUARTER", "start_date").alias("quarter_start"),
    )
    .agg(
        F.max("monthly_fee_avg_5_prior_avg_5_prior").alias("max_monthly_fee_avg_5_prior_avg_5_prior"),
        F.count("*").alias("cnt"),
    )
)

result = (
    totals
    .select(
        "customer_id",
        "activity_month",
        "quarter_start",
        "max_monthly_fee_avg_5_prior_avg_5_prior",
    )
)
