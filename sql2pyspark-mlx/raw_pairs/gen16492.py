from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("store"))))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.dayofweek("ends_on").alias("ends_on_dayofweek"),
        (F.datediff("starts_on", "ends_on") / 7).alias("weeks_ends_on_to_starts_on"),
    )
)

result = (
    filtered
    .select("channel", "id")
    .intersectAll(
        df_promotions
        .filter(F.month("ends_on").isin([2, 4, 12]))
        .select("channel", "id")
    )
)
