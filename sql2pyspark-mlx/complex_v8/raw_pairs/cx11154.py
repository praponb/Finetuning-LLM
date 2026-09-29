from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter(F.col("premium") >= 2319.8)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
    )
)

combined_details_holder = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        F.col("p.segment").alias("holder_segment"),
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
    )
)

screened = (
    combined_details_holder.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.year(F.col("c.claim_date")) == 2024), "left_anti")
    .filter(F.col("end_date") < F.lit("2026-09-19").cast("date"))
)

with_parent_holder = (
    screened.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.holder_segment",
        "s.holder_name",
        "s.holder_region",
        F.col("p.segment").alias("holder_segment_2"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

summary = (
    with_parent_holder
    .filter(F.col("holder_segment").isin("RETAIL", "SME"))
    .groupBy("holder_segment_2")
    .agg(
        F.min("holder_risk_score").alias("min_holder_risk_score"),
        F.datediff(F.max("end_date"), F.min("end_date")).alias("end_date_span_days"),
    )
)

result = (
    summary
    .select(
        "holder_segment_2",
        "end_date_span_days",
        F.round(F.col("min_holder_risk_score") / F.nullif(F.col("end_date_span_days"), F.lit(0)), 3).alias("min_holder_risk_score_per_end_date_span_days"),
        F.length(F.trim(F.col("holder_segment_2"))).alias("holder_segment_2_len"),
    )
    .orderBy(F.col("holder_segment_2").asc(), F.col("holder_segment_2_len").desc())
)
