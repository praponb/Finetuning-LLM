from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_month_adjuster_i = Window.partitionBy(F.month("reported_at"), "adjuster_id")
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_first_known_claim_date_month = Window.partitionBy(F.date_trunc("month", "first_known_claim_date"), "claim_type")

prepared = df_claims.filter((F.col("policy_id") <= 9) & (F.col("status") == "REJECTED"))

ranked = (
    prepared
    .select(
        "*",
        F.count(F.lit(1)).over(w_reported_at_month_adjuster_i).alias("rows_in_reported_at_month_adjuster_id"),
        F.first("claim_date", ignorenulls=True).over(w_all_by_reported_at_running).alias("first_known_claim_date"),
        F.round(F.cume_dist().over(w_all_by_reported_at), 3).alias("reported_at_cume_dist"),
    )
)

enriched_adjuster = (
    ranked.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.rows_in_reported_at_month_adjuster_id",
        "s.first_known_claim_date",
        "s.reported_at_cume_dist",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

ranked_2 = (
    enriched_adjuster
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "rows_in_reported_at_month_adjuster_id",
        "first_known_claim_date",
        "reported_at_cume_dist",
        "adjuster_team",
        F.size(F.collect_set("adjuster_team").over(w_first_known_claim_date_month)).alias("distinct_adjuster_team_in_first_known_claim_date"),
        F.max("reported_at_cume_dist").over(w_first_known_claim_date_month).alias("first_known_claim_date_month_claim_type_max_repo"),
    )
)

result = (
    ranked_2
    .cube("adjuster_team", "distinct_adjuster_team_in_first_known_claim_date", "status")
    .agg(
        F.sum(F.when(F.col("status").isin("APPROVED", "PAID"), F.col("distinct_adjuster_team_in_first_known_claim_date")).otherwise(0)).alias("distinct_adjuster_team_in_first_known_claim_date_2"),
        F.count("*").alias("cnt"),
        F.countDistinct("claim_type").alias("distinct_claim_types"),
    )
    .select(
        F.coalesce(F.col("adjuster_team"), F.lit("ALL")).alias("adjuster_team"),
        "distinct_adjuster_team_in_first_known_claim_date",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "distinct_adjuster_team_in_first_known_claim_date_2",
        "cnt",
        "distinct_claim_types",
    )
)
