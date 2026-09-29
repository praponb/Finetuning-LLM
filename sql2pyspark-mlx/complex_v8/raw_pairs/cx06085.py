from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type = Window.partitionBy("claim_type")
w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_status_reported_at_to_date = Window.partitionBy("status", F.to_date("reported_at"))
w_all_by_claim_date_running = Window.orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_adjuster_id_by_adjust = Window.partitionBy("status", "adjuster_id").orderBy(F.col("adjuster_level").desc())
w_claim_type_policy_id = Window.partitionBy("claim_type", "policy_id")
w_status_adjuster_id_by_adjust_2 = Window.partitionBy("status", "adjuster_id").orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(-2, -1)
w_policy_id = Window.partitionBy("policy_id")
w_claim_date_quartile_policy_s = Window.partitionBy("claim_date_quartile", "policy_status").orderBy("adjuster_level_vs_status_reported_at_to_date_avg", "id")
w_status_adjuster_team_by_adju = Window.partitionBy("status", "adjuster_team").orderBy("adjuster_level_vs_status_reported_at_to_date_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_month_by_claim_ty = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy(F.col("claim_type_policy_id_min_adjuster_level").desc(), "id")
w_claim_date_month_by_claim_ty_2 = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy(F.col("claim_type_policy_id_min_adjuster_level").desc(), "id").rowsBetween(-2, Window.currentRow)
w_claim_date_quartile_policy_s_2 = Window.partitionBy("claim_date_quartile", "policy_status").orderBy("adjuster_level_vs_status_reported_at_to_date_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_adjuster_team_by_adju_2 = Window.partitionBy("status", "adjuster_team").orderBy("adjuster_level_vs_status_reported_at_to_date_avg", "id")

scoped_claims = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.date_add(F.col("claim_date"), 7).alias("claim_date_plus_7d"),
    )
)

enriched_adjuster = (
    scoped_claims.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.adjuster_name").isNotNull()), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.status",
        "s.reported_at",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.status").alias("policy_status"),
        F.col("p.product").alias("policy_product"),
    )
)

windowed = (
    enriched_adjuster
    .select(
        "*",
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_claim_type), 2).alias("adjuster_level_vs_claim_type_avg"),
        F.lead("adjuster_level", 2).over(w_all_by_claim_date).alias("next2_adjuster_level"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_status_reported_at_to_date), 2).alias("adjuster_level_vs_status_reported_at_to_date_avg"),
        F.first("reported_at", ignorenulls=True).over(w_all_by_claim_date_running).alias("first_known_reported_at"),
        F.rank().over(w_status_adjuster_id_by_adjust).alias("adjuster_level_rank"),
        F.size(F.collect_set("policy_product").over(w_status_reported_at_to_date)).alias("distinct_policy_product_in_status_reported_at_to"),
        F.round(F.avg("adjuster_level").over(w_claim_type_policy_id), 2).alias("claim_type_policy_id_avg_adjuster_level"),
        F.min("adjuster_level").over(w_claim_type_policy_id).alias("claim_type_policy_id_min_adjuster_level"),
        F.row_number().over(w_all_by_claim_date).alias("seq_no"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_claim_type_policy_id), 2).alias("adjuster_level_vs_claim_type_policy_id_avg"),
        F.round(F.avg("adjuster_level").over(w_status_adjuster_id_by_adjust_2), 2).alias("adjuster_level_avg_2_prior"),
        F.ntile(4).over(w_all_by_claim_date).alias("claim_date_quartile"),
        F.rank().over(w_all_by_claim_date).alias("claim_date_rank"),
        F.sum("adjuster_level").over(w_all_by_claim_date_running).alias("running_adjuster_level"),
    )
)

top_per_group = windowed.filter(F.col("claim_date_rank") <= 3)

with_trends = (
    top_per_group
    .select(
        "*",
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.round(F.cume_dist().over(w_claim_date_quartile_policy_s), 3).alias("adjuster_level_vs_status_reported_at_to_date_avg_2"),
        F.last("adjuster_level_rank").over(w_status_adjuster_team_by_adju).alias("last_adjuster_level_rank"),
        F.row_number().over(w_claim_date_month_by_claim_ty).alias("seq_no_2"),
        F.round(F.avg("next2_adjuster_level").over(w_claim_date_month_by_claim_ty_2), 2).alias("next2_adjuster_level_avg_3"),
        F.first("claim_date_rank").over(w_claim_date_quartile_policy_s_2).alias("first_claim_date_rank"),
        F.ntile(10).over(w_status_adjuster_team_by_adju_2).alias("adjuster_level_vs_status_reported_at_to_date_avg_3"),
        F.rank().over(w_claim_date_month_by_claim_ty).alias("claim_type_policy_id_min_adjuster_level_rank"),
    )
)

result = (
    with_trends
    .filter(F.hour(F.col("reported_at")).between(10, 13))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
        "adjuster_name",
        "policy_status",
        "policy_product",
        "next2_adjuster_level",
        "adjuster_level_vs_status_reported_at_to_date_avg",
        "adjuster_level_rank",
        "claim_type_policy_id_avg_adjuster_level",
        "claim_type_policy_id_min_adjuster_level",
        "seq_no",
        "adjuster_level_vs_claim_type_policy_id_avg",
        "adjuster_level_avg_2_prior",
        "claim_date_rank",
        "running_adjuster_level",
        "rows_in_policy_id",
        "adjuster_level_vs_status_reported_at_to_date_avg_2",
        "seq_no_2",
        "next2_adjuster_level_avg_3",
        "adjuster_level_vs_status_reported_at_to_date_avg_3",
        (F.col("first_known_reported_at") + F.expr("INTERVAL 15 MINUTES")).alias("first_known_reported_at_plus_15m"),
    )
    .orderBy("rows_in_policy_id", F.col("adjuster_name").desc())
)
