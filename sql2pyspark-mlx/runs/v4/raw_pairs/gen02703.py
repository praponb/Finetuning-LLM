from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("ends_on")
w2 = Window.partitionBy("channel").orderBy(F.col("discount_pct").asc_nulls_first(), "id", "promo_id")

filtered = (
    df_promotions
    .filter(F.col("channel") == "partner")
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        F.round((F.col("discount_pct") * 0.85), 2).alias("adjusted_discount_pct"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_window = (
    latest
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

latest_promotions = (
    with_window
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest_promotions
    .groupBy(
        F.when(F.col("adjusted_discount_pct") >= 89, "HIGH").otherwise("LOW").alias("adjusted_discount_pct_tier"),
        "channel",
    )
    .agg(
        F.min("discount_pct").alias("min_discount_pct"),
        F.percentile_approx("promo_id", [0.25, 0.5, 0.75]).alias("promo_id_quartiles"),
    )
)
