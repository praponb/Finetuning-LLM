from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("start_date_period", "id")
w3 = Window.partitionBy("status").orderBy("start_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2023-10-26").cast("date"))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.date_trunc("week", "start_date").alias("start_date_period"),
        F.round((F.col("monthly_fee") * 1.07), 2).alias("monthly_fee_adjusted"),
    )
)

with_windows = (
    filtered
    .withColumn("last3_max_monthly_fee", F.max("monthly_fee").over(w_grp))
    .withColumn("last3_avg_monthly_fee", F.avg("monthly_fee").over(w_grp))
    .withColumn("last3_min_monthly_fee", F.min("monthly_fee").over(w_grp))
)

filtered_subscriptions = (
    with_windows
    .filter(F.col("monthly_fee") < 160)
    .withColumn("start_date_period_label", F.date_format("start_date_period", "MMM yyyy"))
)

with_gaps = (
    filtered_subscriptions
    .withColumn("is_new_session", F.when(F.col("start_date_period") - F.lag("start_date_period").over(w2) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w3))

result = (
    islands
    .rollup(
        F.when(F.col("monthly_fee_adjusted") >= 72, "LARGE").otherwise("SMALL").alias("monthly_fee_adjusted_tier"),
    )
    .agg(
        F.round(F.sum("last3_max_monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("last3_max_monthly_fee_per_enterprise"),
        F.round(F.avg("last3_avg_monthly_fee"), 2).alias("avg_last3_avg_monthly_fee"),
        F.avg(F.when(F.col("status") == "CANCELLED", F.col("last3_min_monthly_fee"))).alias("cancelled_avg_last3_min_monthly_fee"),
        F.max("start_date_period").alias("last_seen"),
        F.round(F.avg("last3_avg_monthly_fee"), 2).alias("avg_last3_avg_monthly_fee_2"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_rows"),
    )
    .orderBy(F.col("avg_last3_avg_monthly_fee").desc_nulls_last())
)
