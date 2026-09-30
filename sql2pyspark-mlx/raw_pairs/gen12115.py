from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("channel").orderBy("discount_pct_int", "id", "promo_id")
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("channel").orderBy("seq_no", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("next_promotion_discount_pct", "id", "promo_id")
w5 = Window.partitionBy("channel").orderBy("seq_no", "id", "promo_id")
w6 = Window.partitionBy("channel").orderBy(F.col("discount_pct_whole").desc(), "id", "promo_id")
w7 = Window.partitionBy("channel").orderBy("next_promotion_discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("channel").orderBy(F.col("next_discount_pct_whole").desc_nulls_last())

cleaned = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2023-10-25").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.concat_ws("/", "channel", "promo_code").alias("channel_promo_code_key"),
        F.ceil("discount_pct").alias("discount_pct_whole"),
    )
)

cleaned_promotions = (
    cleaned
    .filter((F.col("ends_on") >= F.lit("2023-10-12").cast("date")) & F.col("channel").isNotNull())
    .select("*", F.col("discount_pct").cast("int").alias("discount_pct_int"))
)

attributed = (
    cleaned_promotions
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

scored = (
    attributed
    .select(
        "id",
        "channel",
        "discount_pct_whole",
        "discount_pct_int",
        "promo_id",
        F.lead("promotion_discount_pct").over(w_recent).alias("next_promotion_discount_pct"),
        F.lag("promotion_discount_pct").over(w_recent).alias("prev_promotion_discount_pct"),
        F.lead("discount_pct").over(w_recent).alias("next_discount_pct"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.min("discount_pct_int").over(w2).alias("channel_min_discount_pct_int"),
    )
)

scored_promotions = (
    scored
    .select(
        "*",
        F.sum("prev_promotion_discount_pct").over(w3).alias("running_prev_promotion_discount_pct"),
        F.lag("discount_pct_whole", 1).over(w4).alias("prev_discount_pct_whole"),
        F.lead("discount_pct_whole", 2).over(w5).alias("next_discount_pct_whole"),
        F.min("seq_no").over(w2).alias("channel_min_seq_no"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

enriched = (
    scored_promotions
    .withColumn("cumulative_next_discount_pct_whole", F.sum("next_discount_pct_whole").over(w7))
    .withColumn("next_discount_pct_whole_dense_rank", F.dense_rank().over(w8))
    .withColumn("channel_min_channel_min_discount_pct_int", F.min("channel_min_discount_pct_int").over(w2))
    .withColumn("channel_count_next_discount_pct", F.count("next_discount_pct").over(w2))
    .withColumn("promo_id_share", F.round((F.col("promo_id") / F.sum("promo_id").over(w2)), 4))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("discount_pct_whole") >= 67, "LARGE").otherwise("SMALL").alias("discount_pct_whole_tier"),
        "promo_id",
        "channel",
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.round(F.sum("promo_id") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("promo_id_per_mobile"),
        F.percentile_approx("prev_promotion_discount_pct", 0.9).alias("prev_promotion_discount_pct_p90"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.round(F.avg("prev_discount_pct_whole"), 2).alias("avg_prev_discount_pct_whole"),
    )
    .filter(F.col("partner_events") > 1)
)
