from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc())
w_grp = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("status").orderBy("monthly_fee", "id")
w5 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_last())
w6 = Window.partitionBy("status").orderBy("seq_no", "id")
w7 = Window.partitionBy("status").orderBy("next_first_known_monthly_fee", "id")

valid = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-08-19").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.add_months("end_date", 6).alias("end_date_plus_months"),
        F.date_sub("start_date", 86).alias("start_date_minus_86d"),
    )
)

cleaned = (
    valid
    .filter(F.month("end_date").isin([3, 6, 8]))
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "end_date_plus_months",
        F.when(F.col("monthly_fee") >= 210, "High").when(F.col("monthly_fee") >= 57, "Medium").otherwise("Low").alias("monthly_fee_tier"),
        F.upper("status").alias("status_upper"),
    )
)

enriched = (
    cleaned
    .filter(F.month("start_date").isin([5, 10, 12]))
    .select(
        "*",
        F.percent_rank().over(w).alias("monthly_fee_percent_rank"),
        F.ntile(3).over(w_grp).alias("monthly_fee_bucket"),
        F.ntile(5).over(w_grp).alias("monthly_fee_bucket_2"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.first("monthly_fee", ignorenulls=True).over(w3).alias("first_known_monthly_fee"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "status_upper",
        "monthly_fee_bucket",
        F.lead("monthly_fee_bucket_2").over(w_part).alias("next_monthly_fee_bucket_2"),
        F.lag("monthly_fee_bucket_2").over(w_part).alias("prev_monthly_fee_bucket_2"),
        F.lead("first_known_monthly_fee").over(w_part).alias("next_first_known_monthly_fee"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

filtered = (
    scored
    .filter(F.col("seq_no").between(5, 6))
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "status_upper",
        "monthly_fee_bucket",
        "next_monthly_fee_bucket_2",
        "next_first_known_monthly_fee",
        "seq_no",
        F.coalesce("seq_no", F.lit(0)).alias("seq_no_filled"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched_subscriptions = (
    latest
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "status_upper",
        "monthly_fee_bucket",
        "next_monthly_fee_bucket_2",
        "next_first_known_monthly_fee",
        F.lag("seq_no_filled").over(w6).alias("prev_seq_no_filled"),
        F.lead("monthly_fee_bucket").over(w6).alias("next_monthly_fee_bucket"),
        F.lag("monthly_fee_bucket").over(w6).alias("prev_monthly_fee_bucket"),
        F.row_number().over(w6).alias("seq_no_2"),
        F.lead("seq_no_filled", 1).over(w7).alias("next_seq_no_filled"),
    )
)

result = (
    enriched_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_bucket_tier"),
        F.date_trunc("MONTH", "start_date").alias("activity_month"),
    )
    .agg(
        F.round(F.avg("prev_monthly_fee_bucket"), 2).alias("avg_prev_monthly_fee_bucket"),
        F.percentile_approx("next_monthly_fee_bucket_2", 0.5).alias("next_monthly_fee_bucket_2_p50"),
        F.percentile_approx("next_monthly_fee_bucket", 0.5).alias("next_monthly_fee_bucket_p50"),
    )
    .orderBy(F.col("monthly_fee_bucket_tier").desc())
)
