from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on").desc())
w2 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy(F.col("dedup_rank").desc(), "id")
w4 = Window.partitionBy("promo_code", "channel").orderBy(F.col("last4_avg_discount_pct").asc_nulls_first())
w5 = Window.orderBy(F.col("last4_count_discount_pct").desc())
w6 = Window.partitionBy("promo_code")

filtered = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-07-29").cast("date")) & F.col("discount_pct").between(0.21, 0.29))
    .withColumn("channel_lower", F.lower("channel"))
    .withColumn("years_since_starts_on", (F.datediff(F.current_date(), "starts_on") / 365.25))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    ranked
    .withColumn("last4_max_discount_pct", F.max("discount_pct").over(w2))
    .withColumn("last4_count_discount_pct", F.count("discount_pct").over(w2))
    .withColumn("last4_avg_discount_pct", F.avg("discount_pct").over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

latest = (
    metrics
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

matched_keys = (
    latest
    .select("promo_code")
    .subtract(
        df_promotions
        .filter(F.col("promo_code") == "SPRING10")
        .select("promo_code")
    )
)

verified = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("last4_count_discount_pct_dense_rank", F.dense_rank().over(w5))
    .withColumn("promo_code_sum_event_seq", F.sum("event_seq").over(w6))
)

daily = (
    enriched
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.3, "PREMIUM").otherwise("STANDARD"))
    .withColumn("year_start", F.date_trunc("YEAR", "ends_on"))
    .groupingSets(
        [["discount_pct_tier", "year_start"], ["discount_pct_tier"], []],
        "discount_pct_tier",
        "year_start",
    )
    .agg(
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("promo_code") == "SPRING10", 1)), F.lit(0)), 2).alias("event_seq_per_spring10"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    daily
    .select("discount_pct_tier", "year_start", "avg_event_seq", "grouping_id")
    .orderBy(F.col("year_start").desc())
)
