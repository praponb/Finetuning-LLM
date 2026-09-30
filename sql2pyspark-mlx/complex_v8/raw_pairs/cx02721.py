from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_to_date_by_claim = Window.partitionBy(F.to_date("reported_at")).orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_to_date_by_claim_2 = Window.partitionBy(F.to_date("reported_at")).orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_adjuster_level = Window.orderBy("adjuster_level", "id")
w_adjuster_team_first_adjuster = Window.partitionBy("adjuster_team", "first_adjuster_level")
w_adjuster_level_by_claim_date = Window.partitionBy("adjuster_level").orderBy(F.col("claim_date").desc(), "id")

cleaned_claims = (
    df_claims
    .filter(F.col("status").isNotNull())
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

combined_details = (
    cleaned_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

with_trends = (
    combined_details
    .select(
        "*",
        F.first("adjuster_level").over(w_reported_at_to_date_by_claim).alias("first_adjuster_level"),
        F.max("claimed_amount").over(w_reported_at_to_date_by_claim_2).alias("claimed_amount_max_7"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "adjuster_team",
        "adjuster_level",
        F.round(F.cume_dist().over(w_all_by_adjuster_level), 3).alias("adjuster_level_cume_dist"),
        F.lead("reported_at", 3).over(w_all_by_adjuster_level).alias("next3_reported_at"),
        F.size(F.collect_set("claim_type").over(w_adjuster_team_first_adjuster)).alias("distinct_claim_type_in_adjuster_team_first_adjus"),
        F.row_number().over(w_adjuster_level_by_claim_date).alias("seq_no"),
        F.ntile(3).over(w_all_by_adjuster_level).alias("adjuster_level_tercile"),
    )
)

result = (
    metrics
    .cube("claim_type", "distinct_claim_type_in_adjuster_team_first_adjus")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("claim_type") == "fire", 1).otherwise(0)).alias("n_claim_type_fire"),
    )
    .orderBy(
        F.col("claim_type").asc_nulls_last(),
        F.col("distinct_claim_type_in_adjuster_team_first_adjus").asc_nulls_last(),
    )
)
