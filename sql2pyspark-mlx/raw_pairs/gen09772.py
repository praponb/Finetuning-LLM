from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("starts_on", "id")
w_ordered = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promotion_promo_code").orderBy("promo_id", "id", "promo_id").rowsBetween(-2, -1)
w4 = Window.partitionBy("promo_code").orderBy(F.col("promo_id").asc_nulls_first())
w5 = Window.partitionBy("promo_code")

cleaned = (
    df_promotions
    .filter(F.year("starts_on") == 2023)
    .select(
        "*",
        (F.datediff(F.current_date(), "ends_on") / 365.25).alias("years_since_ends_on"),
        (F.datediff(F.current_date(), "starts_on") / 7).alias("weeks_since_starts_on"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

scored = (
    cleaned
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1).over(w_recent))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w_ordered))
)

matched = (
    scored
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched = (
    matched
    .filter(F.col("promo_code").isin(["WELCOME5", "BUNDLE", "SPRING10"]))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "channel_clean",
        "promo_id",
        "promotion_discount_pct",
        F.avg("years_since_ends_on").over(w3).alias("years_since_ends_on_avg_2_prior"),
        F.dense_rank().over(w4).alias("promo_id_dense_rank"),
        F.max("discount_pct").over(w5).alias("promo_code_max_discount_pct"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("discount_pct") >= 0.34, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
        "promo_code",
    )
    .agg(
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.count_if(F.col("promo_code") == "WELCOME5").alias("welcome5_events"),
        F.round(F.sum("years_since_ends_on_avg_2_prior") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("years_since_ends_on_avg_2_prior_per_partner"),
    )
    .filter(F.col("years_since_ends_on_avg_2_prior_per_partner") >= 0)
)
