from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-10-02").cast("date")) & (F.col("channel") == "store"))
    .select(
        "*",
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
        F.when(F.col("channel").isin(["web", "mobile"]), "Core").otherwise("Other").alias("channel_group"),
    )
)

result = (
    filtered
    .select("promo_code")
    .intersectAll(
        df_promotions
        .filter(F.col("channel") == "web")
        .select("promo_code")
    )
)
