from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "customer_id").orderBy("end_date", "id", "subscription_id").rowsBetween(-3, -1)
w3 = Window.partitionBy("plan").orderBy("start_date_month", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("running_count_monthly_fee", "id", "subscription_id")
w5 = Window.partitionBy("plan")
w6 = Window.partitionBy("plan", "customer_id").orderBy("running_count_monthly_fee", "id", "subscription_id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("end_date").asc_nulls_last(), "id", "subscription_id")

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.last_day("end_date").alias("end_date_month_end"),
    )
)

valid = (
    cleaned
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-06-30"))) & (F.col("plan") == "standard"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "end_date_month_end",
        F.date_format("end_date_month_end", "MMM yyyy").alias("end_date_month_end_label"),
    )
)

enriched = (
    valid
    .filter(F.col("start_date") < "2023-08-29")
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "status",
        F.count("monthly_fee").over(w).alias("running_count_monthly_fee"),
        F.min("monthly_fee").over(w).alias("running_min_monthly_fee"),
        F.max("monthly_fee").over(w).alias("running_max_monthly_fee"),
    )
)

cleaned_subscriptions = (
    enriched
    .filter((F.col("end_date") > F.lit("2023-08-04").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.month("start_date").alias("start_date_month"),
        F.when(F.col("running_count_monthly_fee") >= 13, "High").when(F.col("running_count_monthly_fee") >= 3, "Medium").otherwise("Low").alias("running_count_monthly_fee_tier"),
    )
)

with_window = (
    cleaned_subscriptions
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.end_date") >= F.col("sub.start_date")) & (F.col("src.end_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

enriched_subscriptions = (
    with_window
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "status",
        "running_count_monthly_fee",
        "running_max_monthly_fee",
        "running_count_monthly_fee_tier",
        "subscription_id",
        "monthly_fee",
        F.avg("running_count_monthly_fee").over(w2).alias("running_count_monthly_fee_avg_3_prior"),
        F.sum("running_max_monthly_fee").over(w3).alias("running_running_max_monthly_fee"),
        F.ntile(4).over(w4).alias("running_count_monthly_fee_bucket"),
        F.min("running_count_monthly_fee").over(w5).alias("plan_min_running_count_monthly_fee"),
        F.avg("running_min_monthly_fee").over(w6).alias("running_min_monthly_fee_avg_3"),
    )
)

latest = (
    enriched_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.city").alias("city"), F.col("d.segment").alias("segment"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.plan_min_running_count_monthly_fee")).alias("max_plan_min_running_count_monthly_fee"),
        F.round(F.avg(F.col("src.running_running_max_monthly_fee")), 2).alias("avg_running_running_max_monthly_fee"),
    )
)
