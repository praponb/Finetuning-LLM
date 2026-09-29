from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_holder_ids = Window.partitionBy("distinct_holder_ids")
w_last_end_date_month_distinct = Window.partitionBy(F.month("last_end_date"), "distinct_holder_ids").orderBy("median_premium", "first_claim_date_month")

recent = (
    df_policies
    .filter((F.year(F.col("start_date")) == 2025) | (F.col("end_date").between(F.lit("2025-12-05").cast("date"), F.lit("2027-05-16").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("claim_date").alias("first_claim_date"))
)

summarized_rows = (
    recent.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.first_claim_date")
)

summary = (
    summarized_rows
    .filter(F.year(F.col("end_date")) == 2026)
    .groupBy(
        F.date_trunc("month", "first_claim_date").alias("first_claim_date_month"),
    )
    .agg(
        F.min("premium_bucket").alias("min_premium_bucket"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.percentile_approx("premium", 0.5).alias("median_premium"),
        F.round(F.sum("premium"), 2).alias("total_premium"),
        F.sum(F.when(F.col("product").isin("HOME", "LIFE", "TRAVEL"), F.col("premium_bucket")).otherwise(0)).alias("premium_bucket_product_in_home_life_travel"),
        F.max("end_date").alias("last_end_date"),
    )
)

with_windows = (
    summary
    .select(
        "first_claim_date_month",
        "min_premium_bucket",
        "distinct_holder_ids",
        "median_premium",
        "total_premium",
        "premium_bucket_product_in_home_life_travel",
        "last_end_date",
        F.min("total_premium").over(w_distinct_holder_ids).alias("distinct_holder_ids_min_total_premium"),
        F.ntile(4).over(w_last_end_date_month_distinct).alias("median_premium_quartile"),
    )
)

derived = (
    with_windows
    .select(
        "*",
        F.greatest(F.col("distinct_holder_ids"), F.col("median_premium_quartile")).alias("greatest_distinct_holder_ids_median_premium_quar"),
        F.round(F.col("min_premium_bucket") / F.nullif(F.col("median_premium_quartile"), F.lit(0)), 3).alias("min_premium_bucket_per_median_premium_quartile"),
    )
)

annotated = (
    derived
    .filter(F.hour(F.col("first_claim_date_month")).between(9, 11))
    .select(
        "*",
        F.date_format(F.col("last_end_date"), "yyyy-MM").alias("last_end_date_label"),
        F.date_format(F.col("last_end_date"), "yyyy-MM").alias("last_end_date_label_2"),
    )
)

tagged = (
    annotated
    .select(
        "*",
        F.concat_ws("-", F.col("last_end_date_label"), F.col("last_end_date_label_2")).alias("last_end_date_label_last_end_date_label_2_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("first_claim_date_month")).alias("days_since_first_claim_date_month"),
        F.date_format(F.col("first_claim_date_month"), "yyyy-MM-dd").alias("first_claim_date_month_label"),
    )
)

result = (
    tagged
    .select(
        "first_claim_date_month",
        "distinct_holder_ids",
        "median_premium",
        "premium_bucket_product_in_home_life_travel",
        "last_end_date",
        "greatest_distinct_holder_ids_median_premium_quar",
        "last_end_date_label",
        "last_end_date_label_2",
        "last_end_date_label_last_end_date_label_2_label",
        "days_since_first_claim_date_month",
        F.substring(F.trim(F.col("last_end_date_label")), 1, 2).alias("last_end_date_label_prefix"),
        (F.col("distinct_holder_ids") - F.col("median_premium_quartile")).alias("distinct_holder_ids_minus_median_premium_quartil"),
    )
    .orderBy(
        "last_end_date_label_2",
        F.col("days_since_first_claim_date_month").asc(),
    )
)
