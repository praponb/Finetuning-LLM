from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_team_adjuster_id_by = Window.partitionBy("adjuster_team", "adjuster_id").orderBy(F.col("claimed_amount").desc(), "id")
w_adjuster_team_adjuster_id_by_2 = Window.partitionBy("adjuster_team", "adjuster_id").orderBy(F.col("claimed_amount").desc())
w_claim_type_adjuster_id_by_cl = Window.partitionBy("claim_type", "adjuster_id").orderBy("claimed_amount")
w_reported_at_month = Window.partitionBy(F.month("reported_at"))
w_adjuster_team_adjuster_id_by_3 = Window.partitionBy("adjuster_team", "adjuster_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
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
        "reported_at",
    )
)

enriched_adjuster = (
    recent.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.team").isNotNull())
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.reported_at",
        F.col("a.team").alias("adjuster_team"),
    )
)

metrics = (
    enriched_adjuster
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "adjuster_team",
        F.rank().over(w_adjuster_team_adjuster_id_by).alias("claimed_amount_rank"),
        F.rank().over(w_adjuster_team_adjuster_id_by_2).alias("claimed_amount_rank_2"),
        F.rank().over(w_claim_type_adjuster_id_by_cl).alias("claimed_amount_rank_3"),
        F.ntile(4).over(w_adjuster_team_adjuster_id_by).alias("claimed_amount_quartile"),
        F.count(F.lit(1)).over(w_reported_at_month).alias("rows_in_reported_at_month"),
        F.max("claimed_amount").over(w_adjuster_team_adjuster_id_by_3).alias("max_claimed_amount_so_far"),
    )
)

result = (
    metrics
    .cube("adjuster_team", "claimed_amount_rank_2", "claim_type")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(100 * F.count(F.when(F.col("adjuster_team").isin("Health", "Property", "Motor"), 1)) / F.count("*"), 1).alias("pct_adjuster_team_in_health_prop"),
        F.array_sort(F.collect_set("adjuster_team")).alias("adjuster_team_list"),
    )
    .orderBy(
        F.col("adjuster_team").asc_nulls_last(),
        F.col("claimed_amount_rank_2").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
