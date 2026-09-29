from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(F.col("ends_on") < "2023-03-01")
    .withColumn("days_since_ends_on", F.datediff(F.current_date(), "ends_on"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("OTHER")))
)

result = (
    filtered
    .select("promo_code", "id")
    .exceptAll(
        df_promotions
        .filter(F.col("starts_on") < "2024-06-21")
        .select("promo_code", "id")
    )
)
