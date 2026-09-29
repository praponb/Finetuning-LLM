from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("status").orderBy(F.col("days_since_start_date").desc(), F.col("id").desc())

recent = (
    df_policies
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

screened = (
    recent.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type").isin("delay", "fire")) & (F.col("c.adjuster_id") <= 5), "left_semi")
)

enriched_holder = (
    screened.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.risk_score").alias("holder_risk_score"))
)

combined_details_holder = (
    enriched_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.days_since_start_date",
        "s.holder_risk_score",
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
    )
)

deduped = (
    combined_details_holder
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

derived = (
    deduped
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "start_date",
        "days_since_start_date",
        "holder_name",
        "holder_region",
        F.initcap(F.trim(F.col("holder_region"))).alias("holder_region_clean"),
        F.initcap(F.trim(F.col("holder_name"))).alias("holder_name_clean"),
        F.when(F.col("days_since_start_date") >= 385, "HIGH").when(F.col("days_since_start_date") >= 210, "MEDIUM").otherwise("LOW").alias("days_since_start_date_tier"),
    )
)

line_level_claims = (
    derived.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.approved_amount") > 4868.7), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.days_since_start_date",
        "s.holder_name",
        "s.holder_name_clean",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    line_level_claims
    .select(
        "id",
        "holder_id",
        "start_date",
        "holder_name",
        "holder_name_clean",
        "claim_id",
        "claim_reported_at",
        "claim_approved_amount",
        (F.col("claim_reported_at") + F.expr("INTERVAL 1 DAYS")).alias("claim_reported_at_plus_1d"),
        F.date_trunc("quarter", F.col("claim_reported_at")).alias("claim_reported_at_quarter"),
    )
    .orderBy("claim_id")
)
