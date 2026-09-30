from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium_ties = Window.orderBy("premium")
w_holder_id_end_date_year = Window.partitionBy("holder_id", F.year("end_date"))

filtered = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.concat_ws("|", F.col("product"), F.col("status")).alias("product_status_label"),
    )
)

with_trends = (
    filtered
    .select(
        "id",
        "premium",
        "end_date",
        "status_ref",
        "product_status_label",
        F.round(F.percent_rank().over(w_all_by_premium_ties), 3).alias("premium_percent_rank"),
        F.min("premium").over(w_holder_id_end_date_year).alias("holder_id_end_date_year_min_premium"),
    )
)

scored = (
    with_trends
    .select(
        "id",
        "premium",
        "end_date",
        "status_ref",
        "premium_percent_rank",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

exploded_detail_claims = (
    scored.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.premium_percent_rank",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        "c.claim_type",
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

enriched_adjuster = (
    exploded_detail_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.claim_adjuster_id"), "inner")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "left")
    .filter(F.col("a.team").isNotNull())
    .select(
        "s.id",
        "s.claim_id",
        "s.claim_date",
        "s.claim_status",
        "s.claim_adjuster_id",
        F.col("a.team").alias("adjuster_team"),
        F.col("c.status").alias("claim_status_2"),
    )
)

joined = (
    enriched_adjuster.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .select(
        "s.id",
        "s.claim_id",
        "s.claim_date",
        "s.claim_status",
        "s.claim_adjuster_id",
        "s.claim_status_2",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    joined
    .select(
        "id",
        "claim_id",
        "claim_claimed_amount",
        "claim_approved_amount",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["claim_claimed_amount", "claim_approved_amount"],
        variableColumnName="field",
        valueColumnName="value",
    )
)
