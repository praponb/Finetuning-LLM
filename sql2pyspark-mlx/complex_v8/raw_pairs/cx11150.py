from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_year_claim_type = Window.partitionBy(F.year("reported_at"), "claim_type")
w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(-2, -1)
w_policy_id_by_claimed_amount_2 = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_claimed_amount_3 = Window.partitionBy("policy_id").orderBy("claimed_amount", "id")
w_adjuster_id_by_least_claimed = Window.partitionBy("adjuster_id").orderBy("least_claimed_amount_approved_amount", "id")
w_policy_id_claim_type = Window.partitionBy("policy_id", "claim_type")
w_adjuster_id_by_least_claimed_2 = Window.partitionBy("adjuster_id").orderBy("least_claimed_amount_approved_amount", "id").rowsBetween(-6, -1)
w_adjuster_id_by_least_claimed_3 = Window.partitionBy("adjuster_id").orderBy("least_claimed_amount_approved_amount", "id").rowsBetween(-1, Window.currentRow)
w_adjuster_id_by_least_claimed_4 = Window.partitionBy("adjuster_id").orderBy("least_claimed_amount_approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_claims = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
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
        F.when(F.col("status").isin("OPEN", "APPROVED"), "group_a").when(F.col("status") == "REJECTED", "group_b").otherwise("other").alias("status_group"),
        F.year(F.col("reported_at")).alias("reported_at_year"),
        F.least(F.col("claimed_amount"), F.col("approved_amount")).alias("least_claimed_amount_approved_amount"),
    )
)

with_activity = (
    recent_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") < 17759.3), "left_anti")
)

sequenced = (
    with_activity
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "status_group",
        "least_claimed_amount_approved_amount",
        F.round(F.avg("reported_at_year").over(w_reported_at_year_claim_type), 2).alias("reported_at_year_claim_type_avg_reported_at_year"),
        F.max("reported_at_year").over(w_policy_id_by_claimed_amount).alias("reported_at_year_max_2_prior"),
        F.first("reported_at").over(w_policy_id_by_claimed_amount_2).alias("first_reported_at"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_policy_id_by_claimed_amount_3)).alias("approved_amount_change"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.dense_rank().over(w_adjuster_id_by_least_claimed).alias("least_claimed_amount_approved_amount_dense_rank"),
        F.count(F.lit(1)).over(w_policy_id_claim_type).alias("rows_in_policy_id_claim_type"),
        F.max("approved_amount_change").over(w_adjuster_id_by_least_claimed_2).alias("approved_amount_change_max_6_prior"),
        F.round(F.avg("reported_at_year_max_2_prior").over(w_adjuster_id_by_least_claimed_3), 2).alias("reported_at_year_max_2_prior_avg_2"),
        F.first("reported_at", ignorenulls=True).over(w_adjuster_id_by_least_claimed_4).alias("first_known_reported_at"),
        F.round(F.col("reported_at_year_max_2_prior") - F.avg("reported_at_year_max_2_prior").over(w_policy_id_claim_type), 2).alias("reported_at_year_max_2_prior_vs_policy_id_claim_"),
    )
)

screened = (
    metrics
    .crossJoin(metrics.agg(F.max("reported_at_year_max_2_prior_vs_policy_id_claim_").alias("overall_max_reported_at_year_max_2_prior_vs_poli")))
    .filter(F.col("reported_at_year_max_2_prior_vs_policy_id_claim_") < F.col("overall_max_reported_at_year_max_2_prior_vs_poli"))
    .drop("overall_max_reported_at_year_max_2_prior_vs_poli")
)

result = (
    screened
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "status_group",
        "least_claimed_amount_approved_amount",
        "reported_at_year_claim_type_avg_reported_at_year",
        "reported_at_year_max_2_prior",
        "first_reported_at",
        "approved_amount_change",
        "least_claimed_amount_approved_amount_dense_rank",
        "rows_in_policy_id_claim_type",
        "approved_amount_change_max_6_prior",
        "reported_at_year_max_2_prior_avg_2",
        "first_known_reported_at",
        "reported_at_year_max_2_prior_vs_policy_id_claim_",
        F.date_trunc("year", F.col("first_known_reported_at")).alias("first_known_reported_at_year"),
    )
    .orderBy(
        "first_reported_at",
        F.col("least_claimed_amount_approved_amount").desc(),
    )
)
