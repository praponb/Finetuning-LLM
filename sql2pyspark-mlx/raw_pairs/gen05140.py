from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id")
w2 = Window.partitionBy("channel", "promotion_promo_code").orderBy("ends_on", "id", "promo_id")
w3 = Window.partitionBy(F.month("ends_on"), "channel").orderBy(F.col("discount_pct").desc())
w4 = Window.partitionBy("promo_code", "channel").orderBy("seq_no", "id", "promo_id")

latest = (
    df_promotions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

attributed = (
    latest
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched = (
    attributed
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "rank_in_group",
        "promo_id",
        "promotion_promo_code",
        F.lag("promotion_discount_pct").over(w2).alias("prev_promotion_discount_pct"),
        F.lag("promo_id").over(w2).alias("prev_promo_id"),
        F.row_number().over(w2).alias("seq_no"),
        F.cume_dist().over(w3).alias("discount_pct_cume_dist"),
    )
)

filtered = (
    enriched
    .filter(F.col("starts_on") > F.lit("2023-09-14").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "rank_in_group",
        "promo_id",
        "promotion_promo_code",
        "prev_promotion_discount_pct",
        "prev_promo_id",
        "seq_no",
        "discount_pct_cume_dist",
        F.ceil("prev_promo_id").alias("prev_promo_id_whole"),
        F.coalesce("promotion_promo_code", F.lit("UNKNOWN")).alias("promotion_promo_code_filled"),
    )
)

with_windows = (
    filtered
    .filter(F.year("starts_on") >= 2025)
    .select(
        "id",
        "channel",
        "promo_code",
        "rank_in_group",
        "promo_id",
        "prev_promotion_discount_pct",
        "seq_no",
        "discount_pct_cume_dist",
        "promotion_promo_code_filled",
        F.lag("prev_promotion_discount_pct").over(w4).alias("prev_prev_promotion_discount_pct"),
        F.lead("prev_promotion_discount_pct").over(w4).alias("next_prev_promotion_discount_pct"),
        F.lead("discount_pct_cume_dist").over(w4).alias("next_discount_pct_cume_dist"),
        F.lag("discount_pct_cume_dist").over(w4).alias("prev_discount_pct_cume_dist"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

result = (
    with_windows
    .withColumn("discount_pct_cume_dist_tier", F.when(F.col("discount_pct_cume_dist") >= 0.77, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["promo_id", "discount_pct_cume_dist_tier", "seq_no"], ["promo_id", "discount_pct_cume_dist_tier"], ["promo_id"], []],
        "promo_id",
        "discount_pct_cume_dist_tier",
        "seq_no",
    )
    .agg(
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.grouping_id().alias("gid"),
    )
)
