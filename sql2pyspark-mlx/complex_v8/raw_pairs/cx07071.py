from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium_bucket = Window.orderBy("premium_bucket", "id")
w_status_product_by_end_date_f = Window.partitionBy("status", "product").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_dedup = Window.partitionBy("status_or_unknown").orderBy("start_date_month_end", "id")
w_claim_count = Window.partitionBy("claim_count")
w_product_by_start_date = Window.partitionBy("product").orderBy(F.col("start_date").asc_nulls_last(), "id")
w_status = Window.partitionBy("status")

scoped_policys = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-03-21").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.last_day(F.col("start_date")).alias("start_date_month_end"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

with_trends = (
    scoped_policys
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_premium_bucket), 3).alias("premium_bucket_percent_rank"),
        F.nth_value("start_date_month_end", 2).over(w_status_product_by_end_date_f).alias("second_start_date_month_end"),
        (F.col("premium") - F.lag("premium").over(w_all_by_premium_bucket)).alias("premium_change"),
    )
)

unique_rows = (
    with_trends
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.count("*").alias("claim_count"),
    )
)

with_counts = (
    unique_rows.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.last_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

with_windows = (
    with_counts
    .withColumn("premium_bucket_share_of_claim_count", F.round(F.col("premium_bucket") / F.nullif(F.sum("premium_bucket").over(w_claim_count), F.lit(0)), 4))
    .withColumn("claim_count_min_claim_count", F.min("claim_count").over(w_claim_count))
)

windowed = (
    with_windows
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        "status_or_unknown",
        "start_date_month_end",
        "premium_bucket",
        "premium_bucket_percent_rank",
        "second_start_date_month_end",
        "premium_change",
        "last_claim_reported_at",
        "claim_count",
        F.lag("end_date").over(w_product_by_start_date).alias("prev_end_date"),
        F.sum("claim_count").over(w_status).alias("status_sum_claim_count"),
        F.round(F.col("premium_change") - F.avg("premium_change").over(w_status), 2).alias("premium_change_vs_status_avg"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.round(F.col("premium_bucket") / F.nullif(F.sum("premium_bucket").over(w_status), F.lit(0)), 4).alias("premium_bucket_share_of_status"),
    )
)

result = (
    windowed
    .filter(F.col("end_date").between(F.lit("2025-09-27").cast("date"), F.lit("2026-11-25").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "status_or_unknown",
        "start_date_month_end",
        "second_start_date_month_end",
        "premium_change",
        "last_claim_reported_at",
        "prev_end_date",
        "status_sum_claim_count",
        "rows_in_status",
        "premium_bucket_share_of_status",
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
    )
    .orderBy("prev_end_date")
)
