from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc(), "id")

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "*",
        F.ceil("discount_pct").alias("discount_pct_whole"),
        F.datediff("starts_on", "ends_on").alias("days_ends_on_to_starts_on"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
