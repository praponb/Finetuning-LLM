from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w3 = Window.orderBy("discount_pct_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code")
w5 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_avg_5").desc(), "id")
w7 = Window.orderBy(F.col("last_known_discount_pct_avg_5_minus_discount_pct_avg_5").desc(), "id")
w8 = Window.partitionBy("promo_code").orderBy("discount_pct_avg_5", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_cume_dist").desc(), "id")

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("channel") != "store"))
    .select(
        "id",
        "promo_code",
        "discount_pct",
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_5"),
        F.cume_dist().over(w2).alias("discount_pct_cume_dist"),
    )
)

cleaned_promotions = (
    scored
    .filter(F.col("discount_pct").between(0.29, 0.31))
    .select(
        "id",
        "promo_code",
        "discount_pct",
        "channel_filled",
        "discount_pct_avg_5",
        "discount_pct_cume_dist",
        F.when(F.col("discount_pct_cume_dist") >= 0.73, "High").when(F.col("discount_pct_cume_dist") >= 0.18, "Medium").otherwise("Low").alias("discount_pct_cume_dist_level"),
    )
)

with_windows = (
    cleaned_promotions
    .withColumn("first_known_discount_pct_cume_dist", F.first("discount_pct_cume_dist", ignorenulls=True).over(w3))
    .withColumn("discount_pct_avg_5_share", F.round((F.col("discount_pct_avg_5") / F.sum("discount_pct_avg_5").over(w4)), 4))
    .withColumn("last_known_discount_pct_avg_5", F.last("discount_pct_avg_5", ignorenulls=True).over(w5))
)

cleaned_promotions_2 = (
    with_windows
    .filter(F.col("discount_pct_cume_dist") > 0.6)
    .select(
        "*",
        F.lower("promo_code").alias("promo_code_lower"),
        (F.col("last_known_discount_pct_avg_5") - F.col("discount_pct_avg_5")).alias("last_known_discount_pct_avg_5_minus_discount_pct_avg_5"),
    )
)

enriched = (
    cleaned_promotions_2
    .select(
        "id",
        "promo_code",
        "discount_pct",
        "discount_pct_avg_5",
        "discount_pct_cume_dist",
        "last_known_discount_pct_avg_5",
        "promo_code_lower",
        F.row_number().over(w6).alias("event_seq"),
        F.row_number().over(w7).alias("event_seq_2"),
        F.avg("last_known_discount_pct_avg_5").over(w8).alias("last_known_discount_pct_avg_5_avg_6"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w9))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .withColumn("last_known_discount_pct_avg_5_tier", F.when(F.col("last_known_discount_pct_avg_5") >= 0, "HIGH").otherwise("LOW"))
    .withColumn("last_known_discount_pct_avg_5_avg_6_tier", F.when(F.col("last_known_discount_pct_avg_5_avg_6") >= 0, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["last_known_discount_pct_avg_5_tier", "last_known_discount_pct_avg_5_avg_6_tier"], ["last_known_discount_pct_avg_5_tier"], []],
        "last_known_discount_pct_avg_5_tier",
        "last_known_discount_pct_avg_5_avg_6_tier",
    )
    .agg(
        F.count_if(F.col("promo_code") == "WELCOME5").alias("welcome5_events"),
        F.sum(F.when(F.col("promo_code") == "SPRING10", F.col("event_seq")).otherwise(0)).alias("spring10_event_seq"),
        F.grouping_id().alias("grouping_level"),
    )
)
