from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("discount_pct")
w3 = Window.partitionBy("promotion_promo_code").orderBy(F.col("discount_pct").desc(), "id", "promo_id")
w4 = Window.partitionBy("channel", "promotion_promo_code").orderBy(F.col("starts_on").desc(), "id", "promo_id", "promo_id_2")

cleaned = (
    df_promotions
    .filter(F.col("promo_code") == "VIP")
    .withColumn("promo_code_channel_key", F.concat_ws(",", "promo_code", "channel"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("NONE")))
)

with_windows = (
    cleaned
    .filter(F.col("promo_code") == "VIP")
    .select(
        "*",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.cume_dist().over(w2).alias("discount_pct_cume_dist"),
    )
)

with_window = (
    with_windows
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

deduped = (
    with_window
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

with_window_promotions = (
    deduped
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("e.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

latest = (
    with_window_promotions
    .withColumn("rank_in_group_2", F.row_number().over(w4))
    .filter(F.col("rank_in_group_2") == 1)
)

kept_keys = (
    latest
    .select("id", "channel")
    .intersect(
        df_promotions
        .filter(~F.col("promo_code").isin(["VIP", "SPRING10"]))
        .select("id", "channel")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("promo_id_2") >= 9, "PREMIUM").otherwise("STANDARD").alias("promo_id_2_tier"),
        F.date_trunc("WEEK", "ends_on").alias("week_start"),
    )
    .agg(
        F.size(F.collect_set("promotion_promo_code_2")).alias("num_promotion_promo_code_2s"),
        F.percentile_approx("discount_pct", 0.5).alias("discount_pct_p50"),
    )
    .orderBy(F.col("num_promotion_promo_code_2s").desc_nulls_last())
)
