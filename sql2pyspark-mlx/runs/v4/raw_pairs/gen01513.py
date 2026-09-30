from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("discount_pct").desc())
w2 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id")
w5 = Window.partitionBy("channel", "promo_code").orderBy("running_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel", "promo_code").orderBy("cumulative_discount_pct", "id")
w7 = Window.partitionBy("promo_code").orderBy("prev_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("promo_code") == "WELCOME5")
    .withColumn("clean_promo_code", F.trim("promo_code"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.percent_rank().over(w_recent).alias("discount_pct_percent_rank"),
        F.sum("discount_pct").over(w2).alias("running_discount_pct"),
        F.sum("discount_pct").over(w3).alias("cumulative_discount_pct"),
        F.lag("discount_pct", 2, 0).over(w4).alias("prev_discount_pct"),
    )
)

with_windows_promotions = (
    with_windows
    .select(
        "*",
        F.last("channel", ignorenulls=True).over(w5).alias("last_known_channel"),
        F.lead("prev_discount_pct", 1).over(w6).alias("next_prev_discount_pct"),
        F.last("channel", ignorenulls=True).over(w7).alias("last_known_channel_2"),
    )
)

attributed = (
    with_windows_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window = (
    attributed
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

daily = (
    with_window
    .groupBy(F.date_trunc("MONTH", "ends_on").alias("activity_month"), "channel")
    .agg(
        F.sort_array(F.collect_set("promotion_promo_code")).alias("promotion_promo_code_list"),
        F.countDistinct("promotion_promo_code").alias("distinct_promotion_promo_codes"),
        F.sum(F.when(F.col("promo_code") == "SPRING10", F.col("prev_discount_pct")).otherwise(0)).alias("spring10_prev_discount_pct"),
        F.percentile_approx("cumulative_discount_pct", 0.95).alias("cumulative_discount_pct_p95"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("distinct_promotion_promo_codes") < 1)
)

result = (
    daily
    .select(
        "activity_month",
        "channel",
        "promotion_promo_code_list",
        "spring10_prev_discount_pct",
        "cumulative_discount_pct_p95",
        "num_rows",
        F.when(F.col("channel").rlike("^(web|partner)$"), "GROUP_A").otherwise("EXTENDED").alias("channel_zone"),
    )
    .orderBy(F.col("cumulative_discount_pct_p95").desc_nulls_first())
)
