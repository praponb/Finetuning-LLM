from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy(F.col("claimed_amount").desc_nulls_first(), "id")
w_claim_type_reported_at_month = Window.partitionBy("claim_type", F.month("reported_at"))
w_reported_at_month_claim_type = Window.partitionBy(F.date_trunc("month", "reported_at"), "claim_type").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_month_claim_type_2 = Window.partitionBy(F.date_trunc("month", "reported_at"), "claim_type").orderBy("approved_amount", "id")
w_adjuster_id_by_reported_at_r = Window.partitionBy("adjuster_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_by_reported_at = Window.partitionBy("adjuster_id").orderBy("reported_at", "id")
w_reported_at_month_claim_type_3 = Window.partitionBy(F.date_trunc("month", "reported_at"), "claim_type").orderBy("approved_amount")
w_claim_date_year_by_reported = Window.partitionBy(F.year("claim_date")).orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_year_by_reported_2 = Window.partitionBy(F.year("claim_date")).orderBy("reported_at")
w_claim_date_year_by_reported_3 = Window.partitionBy(F.year("claim_date")).orderBy("reported_at", "id")
w_all_by_claim_type_reported_a = Window.orderBy(F.col("claim_type_reported_at_month_sum_claimed_amount").desc(), "id").rowsBetween(-6, -1)

valid = (
    df_claims
    .filter((F.col("claim_type") != "flood") & (F.col("policy_id") >= 19))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

metrics = (
    valid
    .select(
        "*",
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_policy_id_by_claimed_amount)).alias("claimed_amount_change"),
        F.sum("claimed_amount").over(w_claim_type_reported_at_month).alias("claim_type_reported_at_month_sum_claimed_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_type_reported_at_month), 2).alias("claimed_amount_vs_claim_type_reported_at_month_a"),
        F.last("approved_amount").over(w_reported_at_month_claim_type).alias("last_approved_amount"),
        F.lead("approved_amount", 1, 0).over(w_reported_at_month_claim_type_2).alias("next_approved_amount"),
        F.first("claim_date", ignorenulls=True).over(w_adjuster_id_by_reported_at_r).alias("first_known_claim_date"),
        F.rank().over(w_adjuster_id_by_reported_at).alias("reported_at_rank"),
        F.count(F.lit(1)).over(w_claim_type_reported_at_month).alias("rows_in_claim_type_reported_at_month"),
        F.dense_rank().over(w_reported_at_month_claim_type_3).alias("approved_amount_dense_rank"),
    )
)

top_per_group = (
    metrics
    .filter(F.col("approved_amount_dense_rank") == 1)
    .drop("approved_amount_dense_rank")
)

windowed = (
    top_per_group
    .select(
        "*",
        F.max("reported_at_rank").over(w_claim_date_year_by_reported).alias("max_reported_at_rank_so_far"),
        F.round(F.percent_rank().over(w_claim_date_year_by_reported_2), 3).alias("reported_at_percent_rank"),
        (F.col("claimed_amount_change") - F.lag("claimed_amount_change").over(w_claim_date_year_by_reported_3)).alias("claimed_amount_change_change"),
        F.first("last_approved_amount", ignorenulls=True).over(w_claim_date_year_by_reported).alias("first_known_last_approved_amount"),
        F.sum("approved_amount").over(w_all_by_claim_type_reported_a).alias("approved_amount_sum_6_prior"),
    )
)

stats = (
    windowed
    .filter(F.col("policy_id") >= 24)
    .groupBy(
        "reported_at_rank",
        F.to_date("reported_at").alias("reported_at_day"),
        F.date_trunc("month", "first_known_claim_date").alias("first_known_claim_date_month"),
    )
    .agg(
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.min("reported_at_rank").alias("min_reported_at_rank"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

result = (
    stats
    .select(
        "reported_at_rank",
        "reported_at_day",
        "first_known_claim_date_month",
        "distinct_adjuster_ids",
        F.coalesce(F.col("reported_at_rank"), F.lit(0)).alias("reported_at_rank_filled"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("first_claim_date")).alias("days_since_first_claim_date"),
    )
    .orderBy(F.col("first_known_claim_date_month").asc_nulls_last())
)
