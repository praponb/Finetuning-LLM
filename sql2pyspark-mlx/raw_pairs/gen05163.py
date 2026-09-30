from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("starts_on")).orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("month", "ends_on"), "promotion_promo_code").orderBy("promo_id", "id", "promo_id").rowsBetween(-6, -1)
w3 = Window.partitionBy("promo_code").orderBy("promo_id", "id", "promo_id")
w4 = Window.partitionBy("promo_code").orderBy(F.col("promo_id").desc(), "id", "promo_id")
w5 = Window.orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("promo_code").orderBy("first_known_promo_id", "id", "promo_id")
w7 = Window.partitionBy("promo_code").orderBy("first_known_promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("promo_code")
w9 = Window.partitionBy("promo_code").orderBy("first_known_promo_id_2", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-04-08").cast("date"))
    .select(
        "*",
        F.trim("channel").alias("clean_channel"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

with_window = (
    valid
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

scored = (
    with_window
    .select(
        "*",
        F.first("promo_id", ignorenulls=True).over(w).alias("first_known_promo_id"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_6_prior"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "promo_code",
        "starts_on",
        "clean_channel",
        "channel_filled",
        "promo_id",
        "promotion_discount_pct",
        "first_known_promo_id",
    )
)

scored_promotions = (
    latest
    .select(
        "*",
        F.sum("first_known_promo_id").over(w5).alias("running_first_known_promo_id"),
        F.ntile(4).over(w6).alias("first_known_promo_id_bucket"),
        F.ntile(4).over(w4).alias("promo_id_bucket"),
        F.first("promo_id", ignorenulls=True).over(w7).alias("first_known_promo_id_2"),
    )
)

enriched = (
    scored_promotions
    .select(
        "*",
        F.count("first_known_promo_id").over(w8).alias("promo_code_count_first_known_promo_id"),
        F.sum("promotion_discount_pct").over(w9).alias("running_promotion_discount_pct"),
        F.first("promotion_discount_pct", ignorenulls=True).over(w9).alias("first_known_promotion_discount_pct"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("MONTH", "starts_on").alias("activity_month"))
    .agg(
        F.percentile_approx("running_first_known_promo_id", [0.25, 0.5, 0.75]).alias("running_first_known_promo_id_quartiles"),
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("first_known_promo_id_bucket"))).alias("welcome5_total_first_known_promo_id_bucket"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("promo_id") / F.nullif(F.count(F.when(F.col("promo_code") == "BUNDLE", 1)), F.lit(0)), 2).alias("promo_id_per_bundle"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("welcome5_total_first_known_promo_id_bucket")
)
