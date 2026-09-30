from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_to_date = Window.partitionBy(F.to_date("reported_at"))
w_all_by_claimed_amount_prior = Window.orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(-3, -1)
w_policy_id_adjuster_id_by_gre = Window.partitionBy("policy_id", "adjuster_id").orderBy("greatest_claimed_amount_approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_adjuster_id_by_gre_2 = Window.partitionBy("policy_id", "adjuster_id").orderBy("greatest_claimed_amount_approved_amount", "id")
w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reported_at_month_policy_id = Window.partitionBy(F.month("reported_at"), "policy_id")
w_all_by_claimed_amount = Window.orderBy(F.col("claimed_amount").desc(), "id")
w_claim_date_month_reported_at = Window.partitionBy(F.month("claim_date"), F.date_trunc("month", "reported_at")).orderBy(F.col("claimed_amount_share_of_reported_at_month_policy").desc(), "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_claim_date_month_adjuster_id = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_id").orderBy(F.col("approved_amount_share_of_reported_at_to_date").desc(), "id").rowsBetween(-3, -1)
w_claim_date_month_adjuster_id_2 = Window.partitionBy(F.date_trunc("month", "claim_date"), "adjuster_id").orderBy(F.col("approved_amount_share_of_reported_at_to_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_claims = (
    df_claims
    .filter(F.col("status") == "PAID")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_ref"),
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
    )
)

windowed = (
    cleaned_claims
    .select(
        "*",
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_reported_at_to_date), F.lit(0)), 4).alias("approved_amount_share_of_reported_at_to_date"),
        F.round(F.avg("claimed_amount").over(w_all_by_claimed_amount_prior), 2).alias("claimed_amount_avg_3_prior"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_reported_at_to_date), 2).alias("claimed_amount_vs_reported_at_to_date_avg"),
        F.nth_value("approved_amount", 2).over(w_policy_id_adjuster_id_by_gre).alias("second_approved_amount"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_policy_id_adjuster_id_by_gre_2)).alias("claimed_amount_change"),
        F.first("claimed_amount", ignorenulls=True).over(w_adjuster_id_by_claimed_amoun).alias("first_known_claimed_amount"),
        F.min("approved_amount").over(w_reported_at_month_policy_id).alias("reported_at_month_policy_id_min_approved_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_reported_at_month_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_reported_at_month_policy"),
        F.ntile(5).over(w_all_by_claimed_amount).alias("claimed_amount_quintile"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.lead("reported_at").over(w_claim_date_month_reported_at).alias("next_reported_at"),
        F.round(F.col("first_known_claimed_amount") / F.nullif(F.sum("first_known_claimed_amount").over(w_adjuster_id), F.lit(0)), 4).alias("first_known_claimed_amount_share_of_adjuster_id"),
        F.max("claimed_amount_avg_3_prior").over(w_claim_date_month_adjuster_id).alias("claimed_amount_avg_3_prior_max_3_prior"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.first("claimed_amount").over(w_claim_date_month_adjuster_id_2).alias("first_claimed_amount"),
    )
)

result = (
    sequenced
    .filter(F.col("claimed_amount_avg_3_prior_max_3_prior").between(7595.0, 112525.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "approved_amount_share_of_reported_at_to_date",
        "claimed_amount_vs_reported_at_to_date_avg",
        "second_approved_amount",
        "claimed_amount_change",
        "first_known_claimed_amount",
        "reported_at_month_policy_id_min_approved_amount",
        "claimed_amount_share_of_reported_at_month_policy",
        "next_reported_at",
        "first_known_claimed_amount_share_of_adjuster_id",
        "claimed_amount_avg_3_prior_max_3_prior",
        "first_claimed_amount",
        F.least(F.col("claimed_amount_change"), F.col("second_approved_amount")).alias("least_claimed_amount_change_second_approved_amou"),
    )
    .orderBy(F.col("first_known_claimed_amount").desc())
)
