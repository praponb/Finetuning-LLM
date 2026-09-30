from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("discount_pct", "id", "promo_id")
w3 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w4 = Window.partitionBy("channel").orderBy(F.col("promotion_discount_pct").desc(), "id", "promo_id")
w5 = Window.partitionBy("channel").orderBy(F.col("promo_id").desc())
w6 = Window.partitionBy("channel").orderBy(F.col("num_promotion_promo_codes").desc())

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.trim("channel").alias("clean_channel"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

with_window = (
    cleaned
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows = (
    with_window
    .select(
        "*",
        F.first("promotion_promo_code", ignorenulls=True).over(w).alias("first_known_promotion_promo_code"),
        F.ntile(4).over(w2).alias("discount_pct_bucket"),
        F.dense_rank().over(w3).alias("discount_pct_dense_rank"),
        F.ntile(4).over(w4).alias("promotion_discount_pct_bucket"),
        F.rank().over(w5).alias("promo_id_rank"),
    )
)

matched_keys = (
    with_windows
    .select("promo_code")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct") <= 0.29)
        .select("promo_code")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

totals = (
    reconciled
    .groupBy(
        "channel",
        F.when(F.col("discount_pct") >= 0.26, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.size(F.collect_set("promotion_promo_code")).alias("num_promotion_promo_codes"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .filter(F.col("num_promotion_promo_codes") >= 1)
    .withColumn("num_promotion_promo_codes_rank", F.rank().over(w6))
)

result = (
    totals
    .select(
        "channel",
        "discount_pct_tier",
        "num_promotion_promo_codes",
        "row_count",
        "channel_list",
        "num_promotion_promo_codes_rank",
    )
    .orderBy("num_promotion_promo_codes")
)
