from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_reported_at_month_by_approve = Window.partitionBy(F.month("reported_at")).orderBy("approved_amount", "id")
w_claim_date_month_policy_id = Window.partitionBy(F.date_trunc("month", "claim_date"), "policy_id")
w_claim_date_quarter_reported = Window.partitionBy("claim_date_quarter", F.date_trunc("month", "reported_at"))
w_claim_date_quarter_adjuster = Window.partitionBy("claim_date_quarter", "adjuster_id").orderBy(F.col("claim_date").desc(), "id")
w_all_by_reported_at_moving_6 = Window.orderBy(F.col("reported_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_claim_date_year_by_approved = Window.partitionBy(F.year("claim_date")).orderBy("approved_amount", "id").rowsBetween(-6, Window.currentRow)
w_all_by_reported_at_prior_6 = Window.orderBy(F.col("reported_at").desc(), "id").rowsBetween(-6, -1)
w_all_by_reported_at_2 = Window.orderBy(F.col("reported_at").desc(), "id")
w_claim_date_quarter_adjuster_2 = Window.partitionBy("claim_date_quarter", "adjuster_id").orderBy(F.col("claim_date").desc())

scoped = (
    df_claims
    .filter((F.col("claimed_amount") > 5774.6) | (~F.col("claim_type").isin("collision", "delay")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
    )
)

with_trends = (
    scoped
    .select(
        "*",
        F.lag("claim_date_quarter").over(w_all_by_reported_at).alias("prev_claim_date_quarter"),
        F.lead("claim_date").over(w_reported_at_month_by_approve).alias("next_claim_date"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_date_month_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_claim_date_month_policy"),
        F.count(F.lit(1)).over(w_claim_date_quarter_reported).alias("rows_in_claim_date_quarter_reported_at_month"),
        F.row_number().over(w_claim_date_quarter_adjuster).alias("seq_no"),
        F.max("approved_amount").over(w_all_by_reported_at_moving_6).alias("approved_amount_max_7"),
        F.sum("claim_date_quarter").over(w_claim_date_year_by_approved).alias("claim_date_quarter_sum_7"),
        F.round(F.avg("claim_date_quarter").over(w_all_by_reported_at_prior_6), 2).alias("claim_date_quarter_avg_6_prior"),
        F.min("approved_amount").over(w_claim_date_month_policy_id).alias("claim_date_month_policy_id_min_approved_amount"),
        F.row_number().over(w_all_by_reported_at_2).alias("reported_at_seq"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_date_quarter_reported), 2).alias("approved_amount_vs_claim_date_quarter_reported_a"),
        F.round(F.cume_dist().over(w_claim_date_quarter_adjuster_2), 3).alias("claim_date_cume_dist"),
        F.round(F.col("claim_date_quarter") - F.avg("claim_date_quarter").over(w_claim_date_month_policy_id), 2).alias("claim_date_quarter_vs_claim_date_month_policy_id"),
        F.round(F.col("claim_date_quarter") / F.nullif(F.sum("claim_date_quarter").over(w_claim_date_month_policy_id), F.lit(0)), 4).alias("claim_date_quarter_share_of_claim_date_month_pol"),
    )
)

top_per_group = (
    with_trends
    .filter((F.col("reported_at_seq") <= 2) & (F.col("claim_date_quarter_avg_6_prior") >= 1.4))
)

result = (
    top_per_group
    .select(
        "id",
        "claim_date",
        "approved_amount",
        "reported_at",
        "claim_date_quarter",
        "prev_claim_date_quarter",
        "next_claim_date",
        "approved_amount_share_of_claim_date_month_policy",
        "seq_no",
        "approved_amount_max_7",
        "claim_date_quarter_sum_7",
        "claim_date_quarter_avg_6_prior",
        "claim_date_month_policy_id_min_approved_amount",
        "reported_at_seq",
        "approved_amount_vs_claim_date_quarter_reported_a",
        "claim_date_cume_dist",
        "claim_date_quarter_vs_claim_date_month_policy_id",
        "claim_date_quarter_share_of_claim_date_month_pol",
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
        F.coalesce(F.col("claim_date_quarter"), F.lit(0)).alias("claim_date_quarter_filled"),
    )
    .orderBy(
        F.col("seq_no").asc_nulls_last(),
        F.col("claim_date_cume_dist").desc_nulls_first(),
    )
)
