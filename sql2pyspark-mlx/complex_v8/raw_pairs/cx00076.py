from pyspark.sql import functions as F

filtered = (
    df_policies
    .filter((F.col("end_date") < F.lit("2026-04-25").cast("date")) | (F.col("product").isin("TRAVEL", "HEALTH", "LIFE")))
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

without_matches = (
    filtered.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.start_date") < F.lit("2025-05-28").cast("date")), "left_anti")
)

combined_details_holder = (
    without_matches.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

result = (
    combined_details_holder
    .groupBy("status", "holder_segment")
    .pivot("product", ["HOME", "LIFE", "AUTO", "TRAVEL"])
    .agg(F.sum("premium"))
    .orderBy("status", "holder_segment")
)
