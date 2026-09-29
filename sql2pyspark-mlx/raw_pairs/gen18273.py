from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("promotion_discount_pct").desc_nulls_last(), "id", "promo_id")
w2 = Window.partitionBy("channel").orderBy("ends_on", "id", "promo_id", "promo_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("starts_on")).orderBy(F.col("row_num").desc(), "id", "promo_id", "promo_id_2")
w4 = Window.partitionBy("promotion_promo_code_2", "channel").orderBy("promo_id_2", "id", "promo_id", "promo_id_2")
w5 = Window.partitionBy("promotion_promo_code").orderBy("promo_id_2", "id", "promo_id", "promo_id_2")
w_recent = Window.partitionBy("promotion_promo_code").orderBy("promotion_discount_pct", "id", "promo_id", "promo_id_2")
w_hist = Window.partitionBy("promotion_promo_code", "promotion_promo_code_2").orderBy("promo_id_2", "id", "promo_id", "promo_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("cumulative_event_seq_tier").orderBy(F.col("vip_prev_promo_id").desc())

matched = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

latest = (
    matched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

attributed = (
    latest
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

enriched = (
    attributed
    .select(
        "*",
        F.last("promotion_discount_pct_2", ignorenulls=True).over(w2).alias("last_known_promotion_discount_pct_2"),
        F.row_number().over(w3).alias("event_seq"),
        F.lead("promo_id", 1, 0).over(w4).alias("next_promo_id"),
        F.ntile(4).over(w5).alias("promo_id_2_bucket"),
    )
)

enriched_promotions = (
    enriched
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "row_num",
        "promo_id_2",
        "last_known_promotion_discount_pct_2",
        "event_seq",
        F.lead("last_known_promotion_discount_pct_2").over(w_recent).alias("next_last_known_promotion_discount_pct_2"),
        F.lead("promo_id").over(w_recent).alias("next_promo_id_2"),
        F.lag("promo_id").over(w_recent).alias("prev_promo_id"),
        F.row_number().over(w_recent).alias("event_seq_2"),
        F.sum("event_seq").over(w_hist).alias("cumulative_event_seq"),
    )
)

base = (
    enriched_promotions
    .filter((F.col("ends_on") >= F.lit("2023-12-04").cast("date")) & (F.col("row_num") == 1))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "row_num",
        "promo_id_2",
        "last_known_promotion_discount_pct_2",
        "next_last_known_promotion_discount_pct_2",
        "prev_promo_id",
        "cumulative_event_seq",
        F.round((F.col("event_seq") / F.col("last_known_promotion_discount_pct_2")), 2).alias("event_seq_per_last_known_promotion_discount_pct_2"),
    )
)

filtered = (
    base
    .filter((F.col("starts_on") > F.lit("2023-07-24").cast("date")) & (F.col("next_last_known_promotion_discount_pct_2") <= 0.22))
    .select(
        "id",
        "ends_on",
        "promo_id",
        "promotion_promo_code",
        "row_num",
        "promo_id_2",
        "next_last_known_promotion_discount_pct_2",
        "prev_promo_id",
        "cumulative_event_seq",
        "event_seq_per_last_known_promotion_discount_pct_2",
        F.greatest("last_known_promotion_discount_pct_2", "promo_id").alias("greatest_last_known_promotion_discount_pct_2_promo_id"),
    )
)

summary = (
    filtered
    .groupBy(
        F.when(F.col("cumulative_event_seq") >= 42, "PREMIUM").otherwise("STANDARD").alias("cumulative_event_seq_tier"),
        F.date_trunc("QUARTER", "ends_on").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("event_seq_per_last_known_promotion_discount_pct_2", [0.25, 0.5, 0.75]).alias("event_seq_per_last_known_promotion_discount_pct_2_quartiles"),
        F.count(F.when(F.col("promotion_promo_code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.sum(F.when(F.col("promotion_promo_code") == "VIP", F.col("prev_promo_id")).otherwise(0)).alias("vip_prev_promo_id"),
        F.percentile_approx("event_seq_per_last_known_promotion_discount_pct_2", 0.5).alias("event_seq_per_last_known_promotion_discount_pct_2_p50"),
        F.percentile_approx("cumulative_event_seq", [0.25, 0.5, 0.75]).alias("cumulative_event_seq_quartiles"),
    )
    .filter(F.col("event_seq_per_last_known_promotion_discount_pct_2_p50") > 0)
    .withColumn("vip_prev_promo_id_percent_rank", F.percent_rank().over(w8))
)

result = (
    summary
    .select(
        "cumulative_event_seq_tier",
        "quarter_start",
        "event_seq_per_last_known_promotion_discount_pct_2_quartiles",
        "welcome5_count",
        "vip_prev_promo_id",
        "event_seq_per_last_known_promotion_discount_pct_2_p50",
        "cumulative_event_seq_quartiles",
    )
    .orderBy(F.col("welcome5_count").desc_nulls_first())
)
