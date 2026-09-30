from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("promotion_discount_pct", "id", "promo_id")
w2 = Window.partitionBy(F.year("starts_on")).orderBy("discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("ends_on"), "channel").orderBy("ends_on", "id", "promo_id")
w4 = Window.partitionBy("channel").orderBy(F.col("next_promotion_discount_pct").desc_nulls_last(), "id", "promo_id")
w_running = Window.partitionBy("channel").orderBy("ends_on", "id", "promo_id")
w6 = Window.partitionBy("promotion_promo_code").orderBy("ends_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("ends_on", "id", "promo_id")
w8 = Window.partitionBy("channel").orderBy(F.col("row_num").desc(), "id", "promo_id")
w9 = Window.orderBy(F.col("discount_pct").asc_nulls_first())
w10 = Window.partitionBy(F.year("starts_on"), "promotion_promo_code_2").orderBy("cumulative_promotion_discount_pct", "id", "promo_id", "promo_id_2")
w11 = Window.partitionBy("channel").orderBy("promo_id_2", "id", "promo_id", "promo_id_2").rowsBetween(-3, Window.currentRow)

with_window = (
    df_promotions
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

scored = (
    with_window
    .filter(F.col("promo_code") == "BUNDLE")
    .select(
        "*",
        F.lead("discount_pct", 2).over(w).alias("next_discount_pct"),
        F.sum("promotion_discount_pct").over(w2).alias("cumulative_promotion_discount_pct"),
        F.lead("promotion_discount_pct", 1).over(w3).alias("next_promotion_discount_pct"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest
    .select(
        "*",
        F.lag("promo_id", 2, 0).over(w_running).alias("prev_promo_id"),
        F.last("next_discount_pct", ignorenulls=True).over(w6).alias("last_known_next_discount_pct"),
        F.lead("discount_pct", 1).over(w7).alias("next_discount_pct_2"),
    )
)

latest_promotions = (
    enriched
    .withColumn("row_num_2", F.row_number().over(w8))
    .filter(F.col("row_num_2") == 1)
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "cumulative_promotion_discount_pct",
        "prev_promo_id",
        "last_known_next_discount_pct",
    )
)

matched = (
    latest_promotions
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct"),
    )
)

enriched_promotions = (
    matched
    .select(
        "id",
        "starts_on",
        "promo_id",
        "promotion_discount_pct",
        "cumulative_promotion_discount_pct",
        "prev_promo_id",
        "last_known_next_discount_pct",
        "promo_id_2",
        F.cume_dist().over(w9).alias("discount_pct_cume_dist"),
        F.lead("cumulative_promotion_discount_pct", 2).over(w10).alias("next_cumulative_promotion_discount_pct"),
        F.avg("promo_id_2").over(w11).alias("promo_id_2_avg_3"),
    )
)

result = (
    enriched_promotions
    .groupBy(
        F.when(F.col("promo_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("promo_id_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.round(F.avg("promo_id_2_avg_3"), 2).alias("avg_promo_id_2_avg_3"),
        F.percentile_approx("promotion_discount_pct", [0.25, 0.5, 0.75]).alias("promotion_discount_pct_quartiles"),
        F.round(F.avg("prev_promo_id"), 2).alias("avg_prev_promo_id"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 1000)
    .orderBy("avg_prev_promo_id")
)
