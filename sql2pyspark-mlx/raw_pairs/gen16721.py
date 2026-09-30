from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("days_end_date_to_start_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("end_date"), "plan")
w4 = Window.partitionBy("plan").orderBy(F.col("end_date").desc_nulls_first(), "id")

filtered = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(70, 130) & (F.col("end_date") > F.lit("2023-06-27").cast("date")))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
        F.length("status").alias("status_length"),
    )
)

cleaned = (
    filtered
    .filter((F.col("start_date") >= F.lit("2023-07-23").cast("date")) & (F.col("plan") == "basic"))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "status_title",
        "status_length",
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
        F.date_sub("end_date", 67).alias("end_date_minus_67d"),
    )
)

enriched = (
    cleaned
    .filter(F.col("days_end_date_to_start_date").isNotNull())
    .select(
        "*",
        F.avg("days_end_date_to_start_date").over(w).alias("days_end_date_to_start_date_avg_3"),
        F.last("days_end_date_to_start_date", ignorenulls=True).over(w2).alias("last_known_days_end_date_to_start_date"),
        F.stddev_samp("status_length").over(w3).alias("end_date_month_stddev_samp_status_length"),
    )
)

valid = (
    enriched
    .filter((F.col("start_date") > F.lit("2023-09-14").cast("date")) & F.month("end_date").isin([4, 9, 12]))
    .select(
        "id",
        "plan",
        "end_date",
        "status_length",
        "days_end_date_to_start_date",
        "end_date_minus_67d",
        "days_end_date_to_start_date_avg_3",
        "last_known_days_end_date_to_start_date",
        F.coalesce("status_length", F.lit(0)).alias("status_length_filled"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

stats = (
    ranked
    .groupBy(
        F.when(F.col("days_end_date_to_start_date") >= 76, "LARGE").otherwise("SMALL").alias("days_end_date_to_start_date_tier"),
        "plan",
    )
    .agg(
        F.sum(F.when(F.col("plan") == "premium", F.col("status_length_filled"))).alias("premium_total_status_length_filled"),
        F.count_if(F.col("plan") == "premium").alias("premium_events"),
    )
)

result = (
    stats
    .select("days_end_date_to_start_date_tier", "plan", "premium_events")
    .orderBy(F.col("premium_events").desc(), "days_end_date_to_start_date_tier")
)
