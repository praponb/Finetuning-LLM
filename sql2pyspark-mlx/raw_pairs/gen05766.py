from pyspark.sql import functions as F

base = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2023-08-03").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.round((F.datediff("ends_on", "starts_on") / 30), 1).alias("months_starts_on_to_ends_on"),
    )
)

result = (
    base
    .select(
        "id",
        "promo_code",
        "months_starts_on_to_ends_on",
        F.col("discount_pct").alias("DISCOUNT_PCT_VAL"),
    )
    .unpivot(
        ids=["id", "promo_code"],
        values=["months_starts_on_to_ends_on", "DISCOUNT_PCT_VAL"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
