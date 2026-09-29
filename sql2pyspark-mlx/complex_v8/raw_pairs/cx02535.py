from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_holder_id_adjuster_te = Window.partitionBy("policy_holder_id", "adjuster_team").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-3, Window.currentRow)
w_policy_holder_id_by_policy_e = Window.partitionBy("policy_holder_id").orderBy("policy_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_holder_id_adjuster_te_2 = Window.partitionBy("policy_holder_id", "adjuster_team").orderBy(F.col("approved_amount").desc(), "id")
w_adjuster_team = Window.partitionBy("adjuster_team")
w_policy_holder_id_adjuster_te_3 = Window.partitionBy("policy_holder_id", "adjuster_team").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_holder_id_adjuster_te_4 = Window.partitionBy("policy_holder_id", "adjuster_team").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_holder_id_by_policy_e_2 = Window.partitionBy("policy_holder_id").orderBy("policy_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_policy_end_date_full = Window.orderBy("policy_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_holder_id_by_policy_p = Window.partitionBy("policy_holder_id").orderBy(F.col("policy_premium").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id = Window.partitionBy("policy_id")

scoped_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label"),
    )
)

with_parent = (
    scoped_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.reported_at_label",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.holder_id").alias("policy_holder_id"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

with_windows = (
    with_parent
    .select(
        "*",
        F.round(F.avg("policy_premium").over(w_policy_holder_id_adjuster_te), 2).alias("policy_premium_avg_4"),
        F.last("policy_premium").over(w_policy_holder_id_by_policy_e).alias("last_policy_premium"),
        F.lead("approved_amount", 2).over(w_policy_holder_id_adjuster_te_2).alias("next2_approved_amount"),
        F.sum("approved_amount").over(w_adjuster_team).alias("adjuster_team_sum_approved_amount"),
        F.nth_value("policy_end_date", 2).over(w_policy_holder_id_adjuster_te_3).alias("second_policy_end_date"),
        F.round(F.col("policy_premium") / F.nullif(F.sum("policy_premium").over(w_adjuster_team), F.lit(0)), 4).alias("policy_premium_share_of_adjuster_team"),
        F.first("policy_end_date", ignorenulls=True).over(w_policy_holder_id_adjuster_te_4).alias("first_known_policy_end_date"),
        F.min("policy_premium").over(w_adjuster_team).alias("adjuster_team_min_policy_premium"),
        F.count(F.lit(1)).over(w_policy_holder_id_adjuster_te_4).alias("running_count"),
        F.first("policy_end_date", ignorenulls=True).over(w_policy_holder_id_by_policy_e_2).alias("first_known_policy_end_date_2"),
        F.row_number().over(w_policy_holder_id_adjuster_te_2).alias("approved_amount_seq"),
        F.last("policy_premium").over(w_all_by_policy_end_date_full).alias("last_policy_premium_2"),
        F.nth_value("approved_amount", 2).over(w_policy_holder_id_by_policy_p).alias("second_approved_amount"),
        F.size(F.collect_set("adjuster_team").over(w_policy_id)).alias("distinct_adjuster_team_in_policy_id"),
    )
)

leaders = (
    with_windows
    .filter(F.col("approved_amount_seq") <= 3)
    .drop("approved_amount_seq")
)

result = (
    leaders
    .filter(F.col("approved_amount").between(0.0, 32510.7))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "reported_at_label",
        "policy_premium",
        "policy_holder_id",
        "adjuster_name",
        "adjuster_team",
        "policy_premium_avg_4",
        "last_policy_premium",
        "next2_approved_amount",
        "adjuster_team_sum_approved_amount",
        "second_policy_end_date",
        "policy_premium_share_of_adjuster_team",
        "first_known_policy_end_date",
        "adjuster_team_min_policy_premium",
        "running_count",
        "distinct_adjuster_team_in_policy_id",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("second_policy_end_date")).alias("days_since_second_policy_end_date"),
    )
    .orderBy(F.col("policy_premium_avg_4").desc())
)
