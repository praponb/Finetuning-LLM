from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("promotion_promo_code").orderBy("ends_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2023-10-15").cast("date"))
    .select(
        "*",
        F.lower("channel").alias("channel_lower"),
        F.floor("discount_pct").alias("discount_pct_whole"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

matched = (
    base
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows = (
    matched
    .select(
        "*",
        F.avg("promotion_discount_pct").over(w_grp).alias("running_avg_promotion_discount_pct"),
        F.sum("promotion_discount_pct").over(w_grp).alias("running_sum_promotion_discount_pct"),
        F.max("promotion_discount_pct").over(w_grp).alias("running_max_promotion_discount_pct"),
    )
)

attributed = (
    with_windows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    attributed
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "channel_lower",
        "discount_pct_whole",
        "promo_id",
        "promotion_promo_code",
    )
    .orderBy(F.col("promo_id").desc_nulls_first())
)
