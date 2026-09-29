from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code").orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("week", "ends_on"), "channel").orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

attributed = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & F.col("s.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows = (
    attributed
    .select(
        "*",
        F.sum("discount_pct").over(w).alias("cumulative_discount_pct"),
        F.sum("promo_id").over(w2).alias("running_promo_id"),
    )
)

matched = (
    with_windows
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

filtered = (
    matched
    .filter((F.col("ends_on") >= F.lit("2023-07-06").cast("date")) & F.col("promotion_promo_code").isNotNull())
    .withColumn("promotion_promo_code_2_lower", F.lower("promotion_promo_code_2"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

cleaned = (
    filtered
    .filter((F.col("ends_on") > F.lit("2023-11-07").cast("date")) & F.col("promotion_promo_code_2").isin(["WELCOME5", "VIP"]))
    .withColumn("months_since_ends_on", (F.datediff(F.current_date(), "ends_on") / 30))
    .withColumn("channel_promotion_promo_code_2", F.concat("channel", F.lit(" "), "promotion_promo_code_2"))
    .withColumn("promotion_promo_code_2_filled", F.coalesce("promotion_promo_code_2", F.lit("NONE")))
)

result = (
    cleaned
    .groupBy(F.date_trunc("MONTH", "starts_on").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("promotion_discount_pct")).otherwise(0)).alias("welcome5_promotion_discount_pct"),
        F.count("*").alias("row_count"),
        F.avg(F.when(F.col("promotion_promo_code_2") == "BUNDLE", F.col("promo_id"))).alias("bundle_avg_promo_id"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.size(F.collect_set("promotion_promo_code_2")).alias("num_promotion_promo_code_2s"),
    )
    .orderBy(F.col("activity_month").asc_nulls_last())
)
