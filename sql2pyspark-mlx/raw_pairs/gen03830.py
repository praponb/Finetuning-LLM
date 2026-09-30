from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id", "promo_id")
w_hist = Window.partitionBy("channel", "promotion_promo_code").orderBy("weeks_since_starts_on", "id", "promo_id")

valid = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-11-07").cast("date"))
    .select(
        "*",
        (F.datediff(F.current_date(), "starts_on") / 7).alias("weeks_since_starts_on"),
    )
)

attributed = (
    valid
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

ranked = (
    attributed
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.lead("weeks_since_starts_on").over(w_hist).alias("next_weeks_since_starts_on"),
        F.lag("weeks_since_starts_on").over(w_hist).alias("prev_weeks_since_starts_on"),
        F.lead("promo_id").over(w_hist).alias("next_promo_id"),
        F.lag("promo_id").over(w_hist).alias("prev_promo_id"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

result = (
    with_windows
    .cube("promo_id", "channel")
    .agg(
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_count"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("promo_id"))).alias("mobile_total_promo_id"),
        F.sum(F.when(F.col("promo_code") == "SPRING10", F.col("promo_id")).otherwise(0)).alias("spring10_promo_id"),
        F.grouping("promo_id").alias("grouping_level"),
    )
    .orderBy(F.col("channel").desc())
)
