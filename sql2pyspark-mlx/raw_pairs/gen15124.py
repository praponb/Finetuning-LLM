from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("channel")
w2 = Window.partitionBy("promo_code").orderBy(F.col("weeks_starts_on_to_ends_on").desc_nulls_first())
w3 = Window.partitionBy(F.month("ends_on"), "channel").orderBy("discount_pct", "id")
w4 = Window.partitionBy("channel").orderBy("weeks_starts_on_to_ends_on", "id")
w_running = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w6 = Window.partitionBy("promo_code")
w7 = Window.partitionBy("promo_code").orderBy(F.col("weeks_starts_on_to_ends_on").asc_nulls_last())

base = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        (F.datediff("ends_on", "starts_on") / 7).alias("weeks_starts_on_to_ends_on"),
        F.last_day("starts_on").alias("starts_on_month_end"),
    )
)

with_windows = (
    base
    .filter(F.col("starts_on") > "2025-07-01")
    .withColumn("channel_avg_discount_pct", F.avg("discount_pct").over(w_hist))
    .withColumn("weeks_starts_on_to_ends_on_dense_rank", F.dense_rank().over(w2))
    .withColumn("next_weeks_starts_on_to_ends_on", F.lead("weeks_starts_on_to_ends_on", 2).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("discount_pct_cume_dist", F.cume_dist().over(w_running))
)

scored = (
    with_windows
    .select(
        "*",
        F.count("discount_pct").over(w6).alias("promo_code_count_discount_pct"),
        F.rank().over(w7).alias("weeks_starts_on_to_ends_on_rank"),
    )
)

attributed = (
    scored
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    attributed
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "starts_on_month_end",
        "channel_avg_discount_pct",
        "next_weeks_starts_on_to_ends_on",
        "weeks_starts_on_to_ends_on_rank",
        F.when(F.col("promo_code").rlike("^(BUNDLE|SPRING10)$"), "CORE").otherwise("OTHER_ZONE").alias("promo_code_zone"),
    )
    .orderBy(F.col("starts_on").desc_nulls_first())
)
