from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id = Window.partitionBy("adjuster_id")
w_status_claim_date_year_by_re = Window.partitionBy("status", F.year("claim_date")).orderBy("reported_at", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
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
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.sum("claimed_amount").over(w_policy_id_by_claimed_amount).alias("running_claimed_amount"),
        F.round(F.col("claim_date_quarter") - F.avg("claim_date_quarter").over(w_adjuster_id), 2).alias("claim_date_quarter_vs_adjuster_id_avg"),
        F.first("claimed_amount", ignorenulls=True).over(w_policy_id_by_claimed_amount).alias("first_known_claimed_amount"),
        F.max("approved_amount").over(w_status_claim_date_year_by_re).alias("approved_amount_max_7"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
    )
)

without_matches = (
    with_windows.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
    .filter(F.col("adjuster_id") <= 7)
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.max("claim_date").alias("last_claim_date"))
)

with_counts = (
    without_matches.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.last_claim_date")
)

with_details_adjuster = (
    with_counts.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claim_date_label",
        "s.claim_date_quarter",
        "s.running_claimed_amount",
        "s.first_known_claimed_amount",
        "s.approved_amount_max_7",
        "s.rows_in_adjuster_id",
        "s.last_claim_date",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("p.premium").alias("policy_premium"),
    )
)

result = (
    with_details_adjuster
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        "claim_date_label",
        "claim_date_quarter",
        "running_claimed_amount",
        "approved_amount_max_7",
        "rows_in_adjuster_id",
        "last_claim_date",
        "adjuster_level",
        "adjuster_name",
        F.concat(F.col("adjuster_team"), F.lit("#"), F.col("policy_id").cast("string")).alias("adjuster_team_ref"),
    )
    .orderBy(F.col("claim_date_quarter").desc(), "id")
    .limit(10)
)
