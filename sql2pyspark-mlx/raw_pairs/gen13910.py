from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.month("end_date"), "status").orderBy(F.col("years_since_end_date").desc(), "id")
w3 = Window.partitionBy("plan").orderBy("end_date", "id")
w4 = Window.partitionBy(F.year("start_date"), "status").orderBy(F.col("prev_years_since_end_date").desc(), "id")
w5 = Window.partitionBy("status").orderBy("years_since_end_date_bucket", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("years_since_end_date_bucket_avg_6_tier").orderBy(F.col("paused_count_2").desc())

base = (
    df_subscriptions
    .filter((F.col("start_date") >= F.to_date(F.lit("2023-09-15"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        (F.datediff(F.current_date(), "end_date") / 365).alias("years_since_end_date"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "plan", "start_date", "end_date", "status", "years_since_end_date")
)

with_windows = (
    ranked
    .select(
        "id",
        "start_date",
        "status",
        F.ntile(4).over(w2).alias("years_since_end_date_bucket"),
        F.lag("years_since_end_date", 1).over(w3).alias("prev_years_since_end_date"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "start_date",
        "status",
        "prev_years_since_end_date",
        F.ntile(3).over(w4).alias("prev_years_since_end_date_bucket"),
        F.avg("years_since_end_date_bucket").over(w5).alias("years_since_end_date_bucket_avg_6"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("years_since_end_date_bucket_avg_6") >= 9, "PREMIUM").otherwise("STANDARD").alias("years_since_end_date_bucket_avg_6_tier"),
        "status",
        F.date_trunc("WEEK", "start_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_count"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_count_2"),
        F.round(F.sum("prev_years_since_end_date") / F.nullif(F.count(F.when(F.col("status") == "CANCELLED", 1)), F.lit(0)), 2).alias("prev_years_since_end_date_per_cancelled"),
    )
    .filter(F.col("prev_years_since_end_date_per_cancelled") > 0)
    .withColumn("paused_count_2_percent_rank", F.percent_rank().over(w6))
    .orderBy(F.col("status").asc_nulls_last())
)
