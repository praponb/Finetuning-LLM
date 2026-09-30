from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee_whole_scaled", "id")
w2 = Window.orderBy("monthly_fee_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("monthly_fee_whole", "id").rowsBetween(-5, -1)

filtered = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2024-01-31").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        F.floor("monthly_fee").alias("monthly_fee_whole"),
    )
)

filtered_subscriptions = (
    filtered
    .filter((F.col("end_date") > F.lit("2023-06-06").cast("date")) & F.col("customer_id").isNotNull() & F.col("plan").isin(["enterprise", "basic", "premium"]))
    .select(
        "id",
        "end_date",
        "monthly_fee_whole",
        F.round((F.col("monthly_fee_whole") * 100), 2).alias("monthly_fee_whole_scaled"),
    )
)

metrics = (
    filtered_subscriptions
    .withColumn("monthly_fee_whole_scaled_bucket", F.ntile(4).over(w))
    .withColumn("last_known_monthly_fee_whole_scaled", F.last("monthly_fee_whole_scaled", ignorenulls=True).over(w2))
    .withColumn("monthly_fee_whole_avg_5_prior", F.avg("monthly_fee_whole").over(w3))
)

cleaned = (
    metrics
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-06-02")))
    .select(
        "*",
        (F.col("monthly_fee_whole_avg_5_prior") * F.col("monthly_fee_whole_scaled_bucket")).alias("monthly_fee_whole_avg_5_prior_times_monthly_fee_whole_scaled_bucket"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "end_date",
        "monthly_fee_whole",
        "monthly_fee_whole_scaled_bucket",
        "monthly_fee_whole_avg_5_prior",
        "monthly_fee_whole_avg_5_prior_times_monthly_fee_whole_scaled_bucket",
    )
    .orderBy(F.col("monthly_fee_whole_scaled_bucket").desc_nulls_first())
)
