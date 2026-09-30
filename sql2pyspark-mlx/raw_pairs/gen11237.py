from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date")
w2 = Window.partitionBy(F.month("end_date"), "plan").orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy(F.date_trunc("WEEK", "start_date"), "plan").orderBy("monthly_fee_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("plan").orderBy("last_known_monthly_fee_filled", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("week", "start_date"), "plan")
w6 = Window.orderBy(F.col("monthly_fee_bucket").asc_nulls_last())

cleaned = (
    df_subscriptions
    .filter((F.col("start_date") > F.to_date(F.lit("2023-09-29"))) & (F.col("end_date") <= "2023-03-01"))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
        F.round((F.col("monthly_fee") * 0.85), 2).alias("adjusted_monthly_fee"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    ranked
    .select(
        "id",
        "plan",
        "start_date",
        F.ntile(4).over(w2).alias("monthly_fee_bucket"),
        F.last("monthly_fee_filled", ignorenulls=True).over(w3).alias("last_known_monthly_fee_filled"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.avg("monthly_fee_bucket").over(w4).alias("monthly_fee_bucket_avg_5"),
        F.avg("monthly_fee_bucket").over(w5).alias("start_date_period_avg_monthly_fee_bucket"),
        F.rank().over(w6).alias("monthly_fee_bucket_rank"),
    )
)

summary = (
    with_windows
    .groupBy(
        F.when(F.col("start_date_period_avg_monthly_fee_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("start_date_period_avg_monthly_fee_bucket_tier"),
    )
    .agg(
        F.max("last_known_monthly_fee_filled").alias("max_last_known_monthly_fee_filled"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") >= 1)
)

result = (
    summary
    .select("start_date_period_avg_monthly_fee_bucket_tier")
    .orderBy(F.col("start_date_period_avg_monthly_fee_bucket_tier").asc_nulls_first())
    .limit(10)
)
