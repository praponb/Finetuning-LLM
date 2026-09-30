from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_team_adjuster_level = Window.partitionBy("adjuster_team", "adjuster_level")
w_all_by_claim_date_quarter = Window.orderBy(F.col("claim_date_quarter").desc_nulls_first(), "id")
w_all_by_claim_date_quarter_ti = Window.orderBy(F.col("claim_date_quarter").desc_nulls_first())
w_all_by_claim_date_quarter_ru = Window.orderBy(F.col("claim_date_quarter").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_claims
    .filter((F.col("status") == "IN_REVIEW") & (F.col("claimed_amount") >= 19347.2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

derived = (
    scoped
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "claimed_amount_per_approved_amount",
        "claim_date_quarter",
        F.when(F.col("claimed_amount") >= 42838.9, "A").when(F.col("claimed_amount") >= 12726.7, "B").otherwise("C").alias("claimed_amount_tier"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
        F.least(F.col("approved_amount"), F.col("approved_amount_filled")).alias("least_approved_amount_approved_amount_filled"),
    )
)

combined_details_adjuster = (
    derived.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isin("Health", "Travel")), "inner")
    .select("s.*", F.col("a.team").alias("adjuster_team"))
)

combined_details_adjuster_2 = (
    combined_details_adjuster.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.claimed_amount_per_approved_amount",
        "s.claim_date_quarter",
        "s.claimed_amount_tier",
        "s.claim_type_ref",
        "s.least_approved_amount_approved_amount_filled",
        "s.adjuster_team",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team_2"),
        F.col("a.level").alias("adjuster_level"),
    )
)

windowed = (
    combined_details_adjuster_2
    .select(
        "id",
        "claim_type",
        "claimed_amount",
        "claimed_amount_per_approved_amount",
        "claimed_amount_tier",
        "claim_type_ref",
        "least_approved_amount_approved_amount_filled",
        "adjuster_name",
        "adjuster_team_2",
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_team_adjuster_level), 2).alias("approved_amount_vs_adjuster_team_adjuster_level_"),
        F.lead("claim_date_quarter").over(w_all_by_claim_date_quarter).alias("next_claim_date_quarter"),
        F.round(F.cume_dist().over(w_all_by_claim_date_quarter_ti), 3).alias("claim_date_quarter_cume_dist"),
        F.sum("adjuster_level").over(w_all_by_claim_date_quarter_ru).alias("running_adjuster_level"),
    )
)

qualified = (
    windowed
    .crossJoin(windowed.agg(F.avg("claim_date_quarter_cume_dist").alias("overall_avg_claim_date_quarter_cume_dist")))
    .filter(F.col("claim_date_quarter_cume_dist") > F.col("overall_avg_claim_date_quarter_cume_dist"))
    .drop("overall_avg_claim_date_quarter_cume_dist")
)

stats = (
    qualified
    .filter(F.col("claimed_amount_per_approved_amount") <= 6.5)
    .groupBy("adjuster_team_2", "claim_type")
    .agg(
        F.count(F.when(F.col("adjuster_team_2") == "Property", 1)).alias("n_adjuster_team_2_property"),
        F.round(F.sum("claimed_amount_per_approved_amount") / F.nullif(F.sum("least_approved_amount_approved_amount_filled"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount_to_least_appr"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("approved_amount_vs_adjuster_team_adjuster_level_").alias("max_approved_amount_vs_adjuster_team_adjuster_le"),
    )
    .filter(F.col("max_approved_amount_vs_adjuster_team_adjuster_le") > -40000.0)
    .drop("max_approved_amount_vs_adjuster_team_adjuster_le")
)

result = (
    stats
    .filter(F.col("claimed_amount_per_approved_amount_to_least_appr") >= 4.0)
    .select(
        "adjuster_team_2",
        "claim_type",
        "distinct_ids",
        F.length(F.trim(F.col("adjuster_team_2"))).alias("adjuster_team_2_len"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("distinct_ids").cast("string")).alias("claim_type_ref"),
    )
    .orderBy(F.col("adjuster_team_2").asc_nulls_last())
)
