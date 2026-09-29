from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct")
w2 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("ends_on"), "channel").orderBy("ends_on", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("promo_code", "promotion_promo_code").orderBy(F.col("promotion_discount_pct_whole").desc_nulls_last())
w5 = Window.partitionBy("promo_code").orderBy(F.col("promotion_discount_pct_whole").desc())

metrics = (
    df_promotions
    .withColumn("discount_pct_dense_rank", F.dense_rank().over(w))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w2))
    .withColumn("discount_pct_avg_3", F.avg("discount_pct").over(w3))
)

with_window = (
    metrics
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

kept_keys = (
    with_window
    .select("id")
    .subtract(
        df_promotions
        .filter(F.month("ends_on").isin([3, 4, 8]))
        .select("id")
    )
)

verified = (
    with_window
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter((F.col("ends_on") > F.lit("2023-08-27").cast("date")) & F.col("promotion_promo_code").isNotNull())
    .select(
        "*",
        F.concat("promotion_promo_code", F.lit(" - "), "promo_code").alias("promotion_promo_code_promo_code"),
        F.floor("promotion_discount_pct").alias("promotion_discount_pct_whole"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

scored = (
    base
    .select(
        "*",
        F.percent_rank().over(w4).alias("promotion_discount_pct_whole_percent_rank"),
        F.dense_rank().over(w5).alias("promotion_discount_pct_whole_dense_rank"),
    )
)

result = (
    scored
    .select(
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_dense_rank",
        "first_known_channel",
        "discount_pct_avg_3",
        "promo_id",
        "promotion_promo_code",
        F.when(F.col("channel").rlike("^(web|partner)$"), "EXTENDED").when(F.col("channel").rlike("^(mobile|store)$"), "CORE").otherwise("GROUP_A").alias("channel_zone"),
    )
    .orderBy(F.col("discount_pct").asc_nulls_first(), F.col("channel").desc())
)
