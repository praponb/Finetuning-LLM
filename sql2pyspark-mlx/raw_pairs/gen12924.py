from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("start_date"), "status").orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("status", "plan").orderBy(F.col("monthly_fee").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())
w5 = Window.partitionBy(F.year("start_date"), "plan")
w_ordered = Window.partitionBy("status").orderBy(F.col("monthly_fee_cume_dist").desc_nulls_last())
w_recent = Window.partitionBy("status").orderBy("monthly_fee_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 220)
    .withColumn("end_date_month_end", F.last_day("end_date"))
    .withColumn("status_lower", F.lower("status"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.ntile(5).over(w2).alias("monthly_fee_bucket"),
        F.cume_dist().over(w3).alias("monthly_fee_cume_dist"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    deduped
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "end_date_month_end",
        "monthly_fee_bucket",
        F.min("monthly_fee_cume_dist").over(w5).alias("start_date_year_min_monthly_fee_cume_dist"),
        F.percent_rank().over(w_ordered).alias("monthly_fee_cume_dist_percent_rank"),
        F.sum("monthly_fee_bucket").over(w_recent).alias("cumulative_monthly_fee_bucket"),
    )
)

valid = (
    enriched
    .filter(F.col("end_date") > F.lit("2023-10-10").cast("date"))
    .withColumn("plan_length", F.length("plan"))
    .withColumn("cumulative_monthly_fee_bucket_whole", F.ceil("cumulative_monthly_fee_bucket"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("start_date_year_min_monthly_fee_cume_dist") >= 1, "LARGE").otherwise("SMALL").alias("start_date_year_min_monthly_fee_cume_dist_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_rows"),
    )
    .filter(F.col("monthly_fee_p90") > 0)
)
