from pyspark.sql import functions as F

base = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-09-13").cast("date")) & F.col("channel").isNotNull())
    .withColumn("weeks_ends_on_to_starts_on", (F.datediff("starts_on", "ends_on") / 7))
)

result = (
    base
    .select("channel", "promo_code")
    .intersectAll(
        df_promotions
        .filter(F.col("ends_on") <= "2023-10-01")
        .select("channel", "promo_code")
    )
)
