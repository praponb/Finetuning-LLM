from pyspark.sql import functions as F

base = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-06-08").cast("date"), F.lit("2026-09-19").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("status").isin("ACTIVE", "EXPIRED"), "group_a").when(F.col("status") == "LAPSED", "group_b").otherwise("other").alias("status_group"),
    )
)

summary = (
    base
    .groupBy("status_group", "holder_id")
    .agg(
        F.percentile_approx("premium", 0.5).alias("median_premium"),
        F.count("*").alias("row_count"),
        F.round(F.avg("premium"), 2).alias("avg_premium"),
        F.min("premium").alias("min_premium"),
    )
)

combined_details_holder = (
    summary.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.status_group",
        "s.holder_id",
        "s.row_count",
        "s.avg_premium",
        "s.min_premium",
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    combined_details_holder
    .select(
        "status_group",
        "holder_id",
        "row_count",
        "avg_premium",
        "holder_region",
        F.length(F.trim(F.col("holder_region"))).alias("holder_region_len"),
    )
    .orderBy(F.col("row_count").asc_nulls_last())
)
