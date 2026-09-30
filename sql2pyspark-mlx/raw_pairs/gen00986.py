from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("promotion_discount_pct").desc_nulls_last())
w2 = Window.partitionBy("promotion_promo_code", "channel").orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promotion_promo_code").orderBy("starts_on", "id", "promo_id").rowsBetween(-5, -1)
w4 = Window.partitionBy("channel").orderBy(F.col("promotion_discount_pct").desc(), "id", "promo_id")
w5 = Window.partitionBy("channel", "promotion_promo_code").orderBy("promo_id_avg_5_prior", "id", "promo_id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy(F.col("promotion_discount_pct_percent_rank").asc_nulls_first())

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-08-04"))) & (F.col("starts_on") <= "2025-10-21"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_format("ends_on", "MMM yyyy").alias("ends_on_label"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

matched = (
    base
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

scored = (
    matched
    .withColumn("promotion_discount_pct_percent_rank", F.percent_rank().over(w))
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w2))
    .withColumn("promo_id_avg_5_prior", F.avg("promo_id").over(w3))
)

filtered = (
    scored
    .filter(F.col("ends_on") > F.lit("2023-10-10").cast("date"))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "channel_filled",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "promotion_discount_pct_percent_rank",
        "running_discount_pct",
        "promo_id_avg_5_prior",
        (F.col("promotion_discount_pct") + F.col("promo_id_avg_5_prior")).alias("promotion_discount_pct_plus_promo_id_avg_5_prior"),
    )
)

filtered_promotions = (
    filtered
    .filter(F.col("promo_id") >= 5)
    .withColumn("promotion_discount_pct_percent_rank_minus_promotion_discount_pct_plus_promo_id_avg_5_prior", (F.col("promotion_discount_pct_percent_rank") - F.col("promotion_discount_pct_plus_promo_id_avg_5_prior")))
    .withColumn("promotion_discount_pct_whole", F.floor("promotion_discount_pct"))
)

enriched = (
    filtered_promotions
    .select(
        "id",
        "channel",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "promotion_discount_pct_percent_rank",
        "promo_id_avg_5_prior",
        "promotion_discount_pct_plus_promo_id_avg_5_prior",
        "promotion_discount_pct_percent_rank_minus_promotion_discount_pct_plus_promo_id_avg_5_prior",
        F.ntile(4).over(w4).alias("promotion_discount_pct_bucket"),
        F.avg("running_discount_pct").over(w5).alias("running_discount_pct_avg_6"),
        F.cume_dist().over(w6).alias("promotion_discount_pct_percent_rank_cume_dist"),
    )
)

cleaned = (
    enriched
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct_percent_rank",
        "promotion_discount_pct_plus_promo_id_avg_5_prior",
        "promotion_discount_pct_percent_rank_minus_promotion_discount_pct_plus_promo_id_avg_5_prior",
        "running_discount_pct_avg_6",
        "promotion_discount_pct_percent_rank_cume_dist",
        F.when(F.col("promo_id") >= 10, "High").when(F.col("promo_id") >= 5, "Medium").otherwise("Low").alias("promo_id_band"),
        F.coalesce("promotion_promo_code", F.lit("NONE")).alias("promotion_promo_code_clean"),
    )
)

result = (
    cleaned
    .groupBy("promotion_promo_code")
    .pivot("channel", ["web", "store", "partner"])
    .agg(F.max("promo_id").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
