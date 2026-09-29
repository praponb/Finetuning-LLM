from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_level = Window.partitionBy("adjuster_level")
w_status_claim_date_year_by_ad = Window.partitionBy("status", F.year("claim_date")).orderBy(F.col("adjuster_level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_claims = (
    df_claims
    .filter((F.col("claim_type") == "fire") & (F.col("approved_amount") >= 0.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        F.abs(F.col("claimed_amount") - 19347.2).alias("claimed_amount_distance"),
    )
)

combined_details = (
    filtered_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.status",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

ranked = (
    combined_details
    .select(
        "*",
        F.min("adjuster_level").over(w_adjuster_level).alias("adjuster_level_min_adjuster_level"),
        F.count(F.lit(1)).over(w_status_claim_date_year_by_ad).alias("running_count"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_adjuster_level), 2).alias("adjuster_level_vs_adjuster_level_avg"),
    )
)

result = (
    ranked
    .select(
        "id",
        "running_count",
        "adjuster_level",
        "adjuster_level_min_adjuster_level",
    )
    .unpivot(
        ids=["id"],
        values=["running_count", "adjuster_level", "adjuster_level_min_adjuster_level"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
