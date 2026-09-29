from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_policy_id_b = Window.partitionBy(F.date_trunc("month", "claim_date"), "policy_id").orderBy(F.col("claimed_amount").desc())
w_claim_date_month_policy_id_b_2 = Window.partitionBy(F.date_trunc("month", "claim_date"), "policy_id").orderBy(F.col("claimed_amount").desc(), "id")
w_adjuster_id_claim_date_month = Window.partitionBy("adjuster_id", F.date_trunc("month", "claim_date"))
w_claim_date_month_policy_id_b_3 = Window.partitionBy(F.date_trunc("month", "claim_date"), "policy_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_by_claim_da = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy("claim_date", "id")
w_all_by_claimed_amount_rank = Window.orderBy(F.col("claimed_amount_rank").asc_nulls_last(), "id")
w_reported_at_year_claimed_amo = Window.partitionBy(F.year("reported_at"), "claimed_amount_rank")

valid_claims = (
    df_claims
    .filter((F.col("reported_at") >= F.lit("2025-01-31 11:00:00").cast("timestamp")) & (F.col("adjuster_id") >= 2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.substring(F.trim(F.col("claim_type")), 1, 2).alias("claim_type_prefix"),
    )
)

sequenced = (
    valid_claims
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "claim_type_prefix",
        F.rank().over(w_claim_date_month_policy_id_b).alias("claimed_amount_rank"),
        F.round(F.cume_dist().over(w_claim_date_month_policy_id_b_2), 3).alias("claimed_amount_cume_dist"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_id_claim_date_month), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_id_claim_date_m"),
        F.count(F.lit(1)).over(w_claim_date_month_policy_id_b_3).alias("running_count"),
        F.lag("approved_amount").over(w_claim_date_month_by_claim_da).alias("prev_approved_amount"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id_claim_date_month), 2).alias("approved_amount_vs_adjuster_id_claim_date_month_"),
    )
)

without_matches = (
    sequenced
    .crossJoin(sequenced.agg(F.avg("claimed_amount").alias("overall_avg_claimed_amount")))
    .filter(F.col("claimed_amount") > F.col("overall_avg_claimed_amount"))
    .drop("overall_avg_claimed_amount")
)

with_windows = (
    without_matches
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_claimed_amount_rank), 3).alias("claimed_amount_rank_cume_dist"),
        F.round(F.col("claimed_amount_cume_dist") - F.avg("claimed_amount_cume_dist").over(w_reported_at_year_claimed_amo), 2).alias("claimed_amount_cume_dist_vs_reported_at_year_cla"),
        F.round(F.avg("running_count").over(w_reported_at_year_claimed_amo), 2).alias("reported_at_year_claimed_amount_rank_avg_running"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        "claimed_amount_rank",
        "claimed_amount_cume_dist",
        "prev_approved_amount",
        "approved_amount_vs_adjuster_id_claim_date_month_",
        "claimed_amount_rank_cume_dist",
        "claimed_amount_cume_dist_vs_reported_at_year_cla",
        "reported_at_year_claimed_amount_rank_avg_running",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)
