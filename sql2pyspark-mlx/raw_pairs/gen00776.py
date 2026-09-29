from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("channel_length").desc_nulls_first())
w2 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy(F.col("channel_length_cume_dist").desc(), "id")

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("channel_length", F.length("channel"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("UNKNOWN")))
)

metrics = (
    valid
    .select(
        "id",
        "promo_code",
        "channel_length",
        F.cume_dist().over(w).alias("channel_length_cume_dist"),
        F.last("channel", ignorenulls=True).over(w2).alias("last_known_channel"),
    )
)

filtered = (
    metrics
    .filter(F.col("channel_length_cume_dist").isNotNull())
    .withColumn("promo_code_lower", F.lower("promo_code"))
    .withColumn("clean_promo_code", F.trim("promo_code"))
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "promo_code",
        "channel_length",
        "last_known_channel",
        "promo_code_lower",
        "clean_promo_code",
        "dedup_rank",
    )
    .orderBy(F.col("id").desc())
)
