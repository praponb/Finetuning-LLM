from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter(F.col("end_date") < F.lit("2026-11-25").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.when(F.col("status").isin("LAPSED", "ACTIVE"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
        F.when(F.col("premium") >= 3544.1, "A").when(F.col("premium") >= 2111.3, "B").otherwise("C").alias("premium_tier"),
    )
)

with_details = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score") > 0.2)
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.days_since_start_date",
        "s.status_group",
        "s.premium_tier",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

labeled = (
    with_details
    .select(
        "id",
        "holder_id",
        "end_date",
        "status_group",
        "premium_tier",
        "holder_segment",
        F.least(F.col("days_since_start_date"), F.col("holder_risk_score")).alias("least_days_since_start_date_holder_risk_score"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

result = (
    labeled
    .filter(F.col("end_date") >= F.lit("2025-12-05").cast("date"))
    .select(
        "id",
        "holder_id",
        "end_date",
        "status_group",
        "premium_tier",
        "product_len",
        F.dayofweek(F.col("end_date")).alias("end_date_dayofweek"),
    )
    .orderBy(F.col("product_len").desc_nulls_first())
)
