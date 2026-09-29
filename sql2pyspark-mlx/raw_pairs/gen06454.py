from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(F.col("channel") != "partner")
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        (F.datediff(F.current_date(), "starts_on") / 365).alias("years_since_starts_on"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

result = (
    filtered
    .select(
        "id",
        "promo_code",
        F.col("years_since_starts_on").alias("YEARS_SINCE_STARTS_ON_VAL"),
        F.col("discount_pct").alias("DISCOUNT_PCT"),
    )
    .unpivot(
        ids=["id", "promo_code"],
        values=["YEARS_SINCE_STARTS_ON_VAL", "DISCOUNT_PCT"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
