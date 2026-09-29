from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee_int", "id")
w3 = Window.partitionBy("week_start").orderBy(F.col("avg_running_avg_monthly_fee_int").desc())

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & F.col("customer_id").isin([33, 36]))
    .select(
        "*",
        F.initcap(F.trim("status")).alias("status_title"),
        F.add_months("end_date", 12).alias("end_date_plus_months"),
    )
)

valid = (
    cleaned
    .filter(F.col("customer_id") == 15)
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "status_title",
        "end_date_plus_months",
        F.when(F.col("monthly_fee") >= 206, "High").when(F.col("monthly_fee") >= 47, "Medium").otherwise("Low").alias("monthly_fee_band"),
    )
)

cleaned_subscriptions = (
    valid
    .filter(F.col("customer_id").isNotNull() & (F.col("start_date") >= "2024-03-01"))
    .withColumn("monthly_fee_int", F.col("monthly_fee").cast("int"))
    .withColumn("weeks_start_date_to_end_date", (F.datediff("end_date", "start_date") / 7))
)

scored = (
    cleaned_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "status_title",
        "end_date_plus_months",
        "monthly_fee_band",
        F.avg("monthly_fee_int").over(w).alias("running_avg_monthly_fee_int"),
        F.sum("monthly_fee_int").over(w).alias("running_sum_monthly_fee_int"),
        F.count("monthly_fee_int").over(w).alias("running_count_monthly_fee_int"),
        F.ntile(3).over(w2).alias("monthly_fee_int_bucket"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("week", "end_date_plus_months").alias("week_start"),
        F.when(F.col("running_sum_monthly_fee_int") >= 340, "LARGE").otherwise("SMALL").alias("running_sum_monthly_fee_int_tier"),
        "customer_id",
    )
    .agg(
        F.percentile_approx("running_avg_monthly_fee_int", 0.5).alias("running_avg_monthly_fee_int_p50"),
        F.round(F.avg("running_count_monthly_fee_int"), 2).alias("avg_running_count_monthly_fee_int"),
        F.avg("running_avg_monthly_fee_int").alias("avg_running_avg_monthly_fee_int"),
    )
    .withColumn("avg_running_avg_monthly_fee_int_rank", F.rank().over(w3))
)
