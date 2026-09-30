from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("product") != "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

with_parent_holder = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.holder_name").isNotNull())
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.product_or_unknown",
        "s.days_since_start_date",
        "p.holder_name",
    )
)

result = (
    with_parent_holder
    .groupBy("product")
    .pivot("product_or_unknown", ["LIFE", "HEALTH", "TRAVEL", "AUTO"])
    .agg(F.avg("premium"))
)
