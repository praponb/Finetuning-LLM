from pyspark.sql import functions as F

base = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.concat_ws("/", "channel", "promo_code").alias("channel_promo_code_key"),
        F.round((F.datediff("starts_on", "ends_on") / 30), 1).alias("months_ends_on_to_starts_on"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

result = (
    base
    .select(
        "id",
        "promo_code",
        F.col("discount_pct").alias("DISCOUNT_PCT"),
        "months_ends_on_to_starts_on",
    )
    .unpivot(
        ids=["id", "promo_code"],
        values=["DISCOUNT_PCT", "months_ends_on_to_starts_on"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
