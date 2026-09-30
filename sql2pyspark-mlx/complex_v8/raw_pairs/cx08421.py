from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id = Window.partitionBy("adjuster_id")
w_policy_id_by_reported_at_mov = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_claim_type_by_reported_at = Window.partitionBy("claim_type").orderBy("reported_at", "id")
w_claim_date_month_adjuster_id = Window.partitionBy(F.month("claim_date"), "adjuster_id").orderBy("reported_at")
w_status_by_claim_date_running = Window.partitionBy("status").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_adjuster_id_2 = Window.partitionBy(F.month("claim_date"), "adjuster_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_reported_at = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), "id")
w_policy_id_by_reported_at_ful = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_reported_at_tie = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc())
w_status_by_claim_date = Window.partitionBy("status").orderBy("claim_date", "id")
w_policy_id_claim_type_by_repo = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("reported_at_cume_dist").asc_nulls_last(), "id")
w_claim_date_month = Window.partitionBy(F.month("claim_date"))
w_policy_id_claim_type_by_repo_2 = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("reported_at_cume_dist").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_claims = df_claims.filter(F.col("claim_date") >= F.lit("2024-03-24").cast("date"))

with_trends = (
    recent_claims
    .select(
        "*",
        F.size(F.collect_set("claim_type").over(w_adjuster_id)).alias("distinct_claim_type_in_adjuster_id"),
        F.round(F.avg("approved_amount").over(w_policy_id_by_reported_at_mov), 2).alias("approved_amount_avg_7"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_claim_type_by_reported_at)).alias("days_since_prev_claim_date"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id), 2).alias("approved_amount_vs_adjuster_id_avg"),
        F.rank().over(w_claim_date_month_adjuster_id).alias("reported_at_rank"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id)).alias("distinct_policy_id_in_adjuster_id"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.first("approved_amount", ignorenulls=True).over(w_status_by_claim_date_running).alias("first_known_approved_amount"),
        F.count(F.lit(1)).over(w_claim_date_month_adjuster_id_2).alias("running_count"),
        F.ntile(10).over(w_policy_id_by_reported_at).alias("reported_at_decile"),
        F.last("reported_at").over(w_policy_id_by_reported_at_ful).alias("last_reported_at"),
        F.round(F.percent_rank().over(w_policy_id_by_reported_at_tie), 3).alias("reported_at_percent_rank"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_status_by_claim_date)).alias("claimed_amount_change"),
        F.round(F.cume_dist().over(w_claim_type_by_reported_at), 3).alias("reported_at_cume_dist"),
    )
)

top_per_group = (
    with_trends
    .filter((F.col("reported_at_rank") <= 5) & (F.col("distinct_policy_id_in_adjuster_id") <= 8))
    .drop("reported_at_rank")
)

ranked = (
    top_per_group
    .select(
        "id",
        "policy_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        "distinct_claim_type_in_adjuster_id",
        "approved_amount_avg_7",
        "days_since_prev_claim_date",
        "distinct_policy_id_in_adjuster_id",
        "rows_in_adjuster_id",
        "first_known_approved_amount",
        "running_count",
        "reported_at_decile",
        "last_reported_at",
        "reported_at_percent_rank",
        "claimed_amount_change",
        "reported_at_cume_dist",
        F.lag("reported_at_percent_rank", 3, 0).over(w_policy_id_claim_type_by_repo).alias("prev3_reported_at_percent_rank"),
        F.round(F.col("reported_at_percent_rank") / F.nullif(F.sum("reported_at_percent_rank").over(w_claim_date_month), F.lit(0)), 4).alias("reported_at_percent_rank_share_of_claim_date_mon"),
        F.nth_value("first_known_approved_amount", 2).over(w_policy_id_claim_type_by_repo_2).alias("second_first_known_approved_amount"),
    )
)

totals = (
    ranked
    .groupBy(F.year("reported_at").alias("reported_at_year"), "claim_type")
    .agg(
        F.round(100 * F.count(F.when(F.col("status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_status_is_not_null"),
        F.sum("days_since_prev_claim_date").alias("total_days_since_prev_claim_date"),
    )
)

result = (
    totals
    .filter(F.col("total_days_since_prev_claim_date") < 840)
    .select(
        "reported_at_year",
        "claim_type",
        "pct_status_is_not_null",
        "total_days_since_prev_claim_date",
    )
    .orderBy(F.col("total_days_since_prev_claim_date").desc())
)
