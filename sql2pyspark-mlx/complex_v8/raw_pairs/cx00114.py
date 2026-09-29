from pyspark.sql import functions as F

filtered = (
    df_policies
    .filter((F.col("product") == "HEALTH") & (F.col("holder_id") <= 8))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

joined_holder = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.days_start_date_to_end_date",
        F.col("p.risk_score").alias("holder_risk_score"),
        "p.holder_name",
    )
)

scored = (
    joined_holder
    .select(
        "id",
        "holder_id",
        "start_date",
        "holder_risk_score",
        "holder_name",
        F.round(F.col("holder_risk_score") * 1.5, 2).alias("holder_risk_score_adj"),
        F.date_trunc("month", F.col("end_date")).alias("end_date_month"),
    )
)

annotated = (
    scored
    .filter(F.col("holder_name").isNotNull())
    .select(
        "*",
        F.length(F.trim(F.col("holder_name"))).alias("holder_name_len"),
        F.concat(F.col("holder_name"), F.lit("#"), F.col("id").cast("string")).alias("holder_name_ref"),
    )
)

enriched_holder = (
    annotated.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.holder_risk_score",
        "s.holder_name",
        "s.end_date_month",
        "s.holder_name_len",
        "s.holder_name_ref",
        F.col("p.region").alias("holder_region"),
        F.col("p.holder_name").alias("holder_holder_name"),
    )
)

without_matches = (
    enriched_holder.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") >= 0.2) & (F.lower(F.col("p.holder_name")).like("%uw%")), "left_anti")
)

result = (
    without_matches
    .filter(F.col("start_date") >= F.lit("2025-01-05").cast("date"))
    .select(
        "id",
        "start_date",
        "holder_risk_score",
        "end_date_month",
        "holder_name_len",
        "holder_holder_name",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date_month")).alias("days_since_end_date_month"),
    )
    .orderBy(F.col("holder_name_len").desc())
)
