from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("status") == "CANCELLED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

line_level = (
    base_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id").isNotNull()), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        "c.claim_date",
    )
)

labeled = (
    line_level
    .filter(F.col("claim_id").isNotNull())
    .select(
        "*",
        F.quarter(F.col("start_date")).alias("start_date_quarter"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

result = (
    labeled
    .groupingSets(
        [["claim_adjuster_id", "start_date_quarter"], ["claim_adjuster_id"], []],
        "claim_adjuster_id", "start_date_quarter",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.min("claim_date").alias("first_claim_date"),
        F.min("start_date").alias("first_start_date"),
    )
    .orderBy(
        F.col("claim_adjuster_id").asc_nulls_last(),
        F.col("start_date_quarter").asc_nulls_last(),
    )
)
