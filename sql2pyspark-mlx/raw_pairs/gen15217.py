from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-03-25"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.upper("promo_code").alias("promo_code_upper"),
        F.round((F.datediff("ends_on", "starts_on") / 30), 1).alias("months_starts_on_to_ends_on"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

result = (
    filtered
    .select("id", "promo_code", "discount_pct", "months_starts_on_to_ends_on")
    .unpivot(
        ids=["id", "promo_code"],
        values=["discount_pct", "months_starts_on_to_ends_on"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
