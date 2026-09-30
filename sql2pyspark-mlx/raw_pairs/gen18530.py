from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id")

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("start_date") >= F.lit("2023-04-19").cast("date")))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("monthly_fee") >= 184, "High").when(F.col("monthly_fee") >= 96, "Medium").otherwise("Low").alias("monthly_fee_tier"),
        F.round((F.datediff("end_date", "start_date") / 30), 1).alias("months_start_date_to_end_date"),
    )
)

filtered_subscriptions = (
    filtered
    .filter(F.col("end_date") > F.lit("2023-08-06").cast("date"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_tier",
        F.concat_ws(",", "plan", "status").alias("plan_status_key"),
    )
)

metrics = (
    filtered_subscriptions
    .select(
        "*",
        F.avg("monthly_fee").over(w).alias("running_avg_monthly_fee"),
        F.max("monthly_fee").over(w).alias("running_max_monthly_fee"),
        F.count("monthly_fee").over(w).alias("running_count_monthly_fee"),
        F.ntile(5).over(w2).alias("monthly_fee_bucket"),
    )
)

base = (
    metrics
    .filter(F.col("end_date") > F.to_date(F.lit("2023-07-10")))
    .withColumn("running_count_monthly_fee_per_running_avg_monthly_fee", F.round((F.col("running_count_monthly_fee") / F.col("running_avg_monthly_fee")), 2))
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_tier",
        "running_avg_monthly_fee",
        "running_max_monthly_fee",
        "running_count_monthly_fee",
    )
)
