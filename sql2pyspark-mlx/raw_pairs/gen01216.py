from pyspark.sql import functions as F

base = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.round("discount_pct", 0).alias("discount_pct_rounded"),
        F.when(F.col("discount_pct") >= 0.33, "High").when(F.col("discount_pct") >= 0.09, "Medium").otherwise("Low").alias("discount_pct_tier"),
    )
)

result = (
    base
    .select("channel", "id")
    .subtract(
        df_promotions
        .filter(F.month("starts_on").isin([4, 6, 11]))
        .select("channel", "id")
    )
)
