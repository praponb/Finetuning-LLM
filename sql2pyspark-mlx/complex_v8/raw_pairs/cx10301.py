from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-05-30").cast("date")) & (F.year(F.col("end_date")) == 2026))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.quarter(F.col("end_date")).alias("end_date_quarter"),
    )
)

scored = (
    base_policys
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        "status_or_unknown",
        "days_since_end_date",
        "end_date_quarter",
        F.concat_ws(": ", F.col("status"), F.col("status_or_unknown")).alias("status_status_or_unknown_label"),
        F.concat_ws(" / ", F.col("status_or_unknown"), F.col("status")).alias("status_or_unknown_status_label"),
    )
)

qualified = (
    scored.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product") == "TRAVEL") & (F.col("p.status").isin("ACTIVE", "LAPSED", "EXPIRED")), "left_semi")
)

with_parent = (
    qualified.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") < 0.6), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        "s.days_since_end_date",
        "s.end_date_quarter",
        "s.status_status_or_unknown_label",
        "s.status_or_unknown_status_label",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

holder_segment_stats = (
    with_parent
    .groupBy("holder_segment")
    .agg(F.max("end_date_quarter").alias("max_end_date_quarter"))
)

vs_group = (
    with_parent.alias("s")
    .join(holder_segment_stats.alias("g"), F.col("g.holder_segment") == F.col("s.holder_segment"), "inner")
    .filter(F.col("s.end_date_quarter") == F.col("g.max_end_date_quarter"))
    .select(
        "s.*",
        "g.max_end_date_quarter",
        F.round(F.col("s.end_date_quarter") - F.col("g.max_end_date_quarter"), 2).alias("end_date_quarter_minus_max"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "holder_id",
        "status",
        "days_since_end_date",
        "end_date_quarter",
        "status_status_or_unknown_label",
        "status_or_unknown_status_label",
        "holder_segment",
        "holder_risk_score",
        "end_date_quarter_minus_max",
    )
    .orderBy(F.col("end_date_quarter").desc(), "id")
    .limit(20)
)
