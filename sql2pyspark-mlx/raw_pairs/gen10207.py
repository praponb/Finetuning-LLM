from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-09-15").cast("date")) & (F.col("ends_on") < "2024-12-01"))
    .select("*", F.round("discount_pct", 2).alias("discount_pct_rounded"))
)

result = (
    filtered
    .select("promo_code", "id")
    .exceptAll(
        df_promotions
        .filter(F.col("discount_pct") < 0.14)
        .select("promo_code", "id")
    )
)
