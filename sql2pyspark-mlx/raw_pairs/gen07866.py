from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("channel").orderBy(F.col("running_count_discount_pct").desc(), "id")
w4 = Window.partitionBy(F.month("ends_on"), "channel").orderBy(F.col("running_max_discount_pct").desc_nulls_first())
w5 = Window.partitionBy("promo_code").orderBy("starts_on_period", "id", "promo_id")
w6 = Window.partitionBy("promo_code").orderBy("starts_on_period", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_promotions
    .filter(F.col("ends_on").between("2024-04-09", "2024-10-01"))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.min("discount_pct").over(w).alias("running_min_discount_pct"),
        F.count("discount_pct").over(w).alias("running_count_discount_pct"),
        F.max("discount_pct").over(w).alias("running_max_discount_pct"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.round((F.col("running_count_discount_pct") / F.sum("running_count_discount_pct").over(w2)), 4).alias("running_count_discount_pct_share"),
        F.row_number().over(w3).alias("row_seq"),
        F.rank().over(w4).alias("running_max_discount_pct_rank"),
    )
)

filtered = (
    enriched
    .filter(F.col("ends_on") > F.lit("2023-10-27").cast("date"))
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        "running_min_discount_pct",
        "running_max_discount_pct",
        "running_count_discount_pct_share",
        F.date_trunc("MONTH", "starts_on").alias("starts_on_period"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

matched = (
    filtered
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

flagged = (
    matched
    .withColumn("session_start_flag", F.when(F.col("starts_on_period") - F.lag("starts_on_period").over(w5) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_promotions = flagged.withColumn("session_no", F.sum("session_start_flag").over(w6))

result = (
    sessions_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        "running_min_discount_pct",
        "channel_filled",
        "promo_id",
        "promo_code",
        "promotion_discount_pct",
    )
)
