from pyspark.sql import functions as F

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("promo_code") != "SPRING10"))
    .select(
        "id",
        "starts_on",
        "ends_on",
        F.length("channel").alias("channel_length"),
        F.date_format("starts_on", "yyyy-MM-dd").alias("starts_on_label"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

result = (
    valid
    .select("id")
    .intersectAll(
        df_promotions
        .filter(F.col("channel").isin(["partner", "mobile"]))
        .select("id")
    )
)
