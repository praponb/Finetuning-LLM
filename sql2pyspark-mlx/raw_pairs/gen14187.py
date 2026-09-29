from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("plan").orderBy("months_start_date_to_end_date", "id")
w3 = Window.partitionBy(F.year("start_date"), "customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("months_start_date_to_end_date_bucket", "id")
w5 = Window.orderBy("first_known_months_start_date_to_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy(F.col("months_start_date_to_end_date_bucket").desc(), "id")
w7 = Window.partitionBy("customer_id", "status").orderBy(F.col("start_date").desc_nulls_last())
w8 = Window.partitionBy("cumulative_months_start_date_to_end_date_tier").orderBy(F.col("paused_rows").desc())

valid = (
    df_subscriptions
    .filter((F.col("start_date") >= F.to_date(F.lit("2023-06-17"))) & ~F.col("plan").isin(["standard", "enterprise", "premium"]))
    .withColumn("end_date_day", F.dayofmonth("end_date"))
    .withColumn("months_start_date_to_end_date", F.round((F.datediff("end_date", "start_date") / 30), 1))
)

scored = (
    valid
    .select(
        "id",
        "customer_id",
        "start_date",
        "status",
        F.sum("months_start_date_to_end_date").over(w).alias("cumulative_months_start_date_to_end_date"),
        F.ntile(10).over(w_grp).alias("months_start_date_to_end_date_bucket"),
        F.first("months_start_date_to_end_date", ignorenulls=True).over(w3).alias("first_known_months_start_date_to_end_date"),
    )
)

scored_subscriptions = (
    scored
    .filter(F.col("months_start_date_to_end_date_bucket") >= 7)
    .withColumn("months_start_date_to_end_date_bucket_bucket", F.ntile(10).over(w4))
    .withColumn("first_known_first_known_months_start_date_to_end_date", F.first("first_known_months_start_date_to_end_date", ignorenulls=True).over(w5))
)

latest = (
    scored_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

latest_subscriptions = (
    latest
    .withColumn("dedup_rank_2", F.rank().over(w7))
    .filter(F.col("dedup_rank_2") == 1)
    .select(
        "id",
        "start_date",
        "status",
        "cumulative_months_start_date_to_end_date",
        "months_start_date_to_end_date_bucket_bucket",
        "dedup_rank",
    )
)

result = (
    latest_subscriptions
    .groupBy(
        F.when(F.col("cumulative_months_start_date_to_end_date") >= 375, "PREMIUM").otherwise("STANDARD").alias("cumulative_months_start_date_to_end_date_tier"),
        F.date_trunc("MONTH", "start_date").alias("activity_month"),
    )
    .agg(
        F.round(F.avg("cumulative_months_start_date_to_end_date"), 2).alias("avg_cumulative_months_start_date_to_end_date"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_rows"),
    )
    .withColumn("paused_rows_percent_rank", F.percent_rank().over(w8))
)
