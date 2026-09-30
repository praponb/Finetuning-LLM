from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code").orderBy("starts_on", "id", "promo_id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("promotion_promo_code").orderBy(F.col("discount_pct_adjusted").desc(), "id", "promo_id")
w3 = Window.partitionBy("promotion_promo_code").orderBy(F.col("promo_id").desc_nulls_last(), "id", "promo_id")
w4 = Window.partitionBy("promo_id").orderBy(F.col("event_count_2").desc())

cleaned = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-04-03"))) & (~F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "*",
        F.round((F.col("discount_pct") * 1.07), 2).alias("discount_pct_adjusted"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

matched = (
    cleaned
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

scored = (
    matched
    .select(
        "id",
        "channel",
        "promo_id",
        "promotion_promo_code",
        F.min("discount_pct").over(w).alias("last3_min_discount_pct"),
        F.max("discount_pct").over(w).alias("last3_max_discount_pct"),
        F.count("discount_pct").over(w).alias("last3_count_discount_pct"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    ranked
    .select("id", "channel")
    .exceptAll(
        df_promotions
        .filter(F.col("promo_code").isin(["BUNDLE", "SPRING10", "WELCOME5"]))
        .select("id", "channel")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("event_seq").between(4, 5))
    .select(
        "id",
        "promo_id",
        "promotion_promo_code",
        "last3_min_discount_pct",
        "last3_max_discount_pct",
        "latest_rank",
        (F.col("last3_min_discount_pct") + F.col("last3_count_discount_pct")).alias("last3_min_discount_pct_plus_last3_count_discount_pct"),
        (F.col("event_seq") + F.col("promo_id")).alias("event_seq_plus_promo_id"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

filtered_promotions = (
    filtered
    .filter(F.col("promotion_promo_code").isNotNull() & (F.col("event_seq_plus_promo_id") >= 66))
    .select(
        "*",
        F.col("last3_max_discount_pct").cast("int").alias("last3_max_discount_pct_int"),
        (F.col("last3_min_discount_pct_plus_last3_count_discount_pct") / F.nullif("event_seq_plus_promo_id", F.lit(0))).alias("last3_min_discount_pct_plus_last3_count_discount_pct_per_event_seq_plus_promo_id"),
    )
)

stats = (
    filtered_promotions
    .groupBy(
        "promo_id",
        F.when(F.col("last3_min_discount_pct_plus_last3_count_discount_pct") >= 82, "HIGH").otherwise("LOW").alias("last3_min_discount_pct_plus_last3_count_discount_pct_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.count_if(F.col("promotion_promo_code") == "SPRING10").alias("spring10_events"),
        F.size(F.collect_set("promotion_promo_code")).alias("num_promotion_promo_codes"),
    )
    .withColumn("event_count_2_percent_rank", F.percent_rank().over(w4))
)

result = (
    stats
    .select(
        "promo_id",
        "last3_min_discount_pct_plus_last3_count_discount_pct_tier",
        "event_count",
        "event_count_2",
        "spring10_events",
        "num_promotion_promo_codes",
    )
)
