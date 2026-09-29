from pyspark.sql import functions as F

base = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("channel").eqNullSafe(F.lit("mobile"))))
    .withColumn("channel_length", F.length("channel"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("OTHER")))
)

result = (
    base
    .select("promo_code")
    .subtract(
        df_promotions
        .filter(F.col("promo_code") != "SPRING10")
        .select("promo_code")
    )
)
