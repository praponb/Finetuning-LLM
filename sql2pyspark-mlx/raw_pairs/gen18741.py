from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("promo_code") == "BUNDLE") & (F.col("starts_on") >= F.to_date(F.lit("2023-03-26"))))
    .select(
        "*",
        (F.datediff(F.current_date(), "ends_on") / 30).alias("months_since_ends_on"),
    )
)

result = (
    filtered
    .groupBy("promo_code")
    .pivot("channel", ["store", "mobile"])
    .agg(F.max("discount_pct"))
)
