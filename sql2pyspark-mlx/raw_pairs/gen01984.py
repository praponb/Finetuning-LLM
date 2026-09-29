from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & F.col("promo_code").isin(["VIP", "WELCOME5"]))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.add_months("starts_on", 6).alias("starts_on_plus_months"),
    )
)

result = (
    filtered
    .select("promo_code", "channel")
    .intersect(
        df_promotions
        .filter(F.col("promo_code") == "BUNDLE")
        .select("promo_code", "channel")
    )
)
