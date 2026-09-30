from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter(F.col("end_date").isNotNull())
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

combined_details_holder = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.region").alias("holder_region"))
)

enriched = (
    combined_details_holder
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.abs(F.col("premium") - 3521.8).alias("premium_distance"),
        F.date_add(F.col("end_date"), 30).alias("end_date_plus_30d"),
    )
)

without_matches = (
    enriched
    .crossJoin(enriched.agg(F.min("premium").alias("overall_min_premium")))
    .filter(F.col("premium") > F.col("overall_min_premium"))
    .drop("overall_min_premium")
)

result = (
    without_matches
    .groupBy("status", "end_date")
    .pivot("holder_region", ["West", "East", "North", "Central"])
    .agg(F.avg("premium_distance"))
    .orderBy("status", "end_date")
)
