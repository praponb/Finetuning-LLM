from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(F.col("channel").isin(["web", "store", "partner"]))
    .select(
        "*",
        F.round("discount_pct", 2).alias("discount_pct_rounded"),
        F.datediff("ends_on", "starts_on").alias("days_starts_on_to_ends_on"),
    )
)

result = (
    filtered
    .select("id", "channel")
    .intersectAll(
        df_promotions
        .filter(F.col("starts_on").between("2023-03-01", "2025-09-01"))
        .select("id", "channel")
    )
)
