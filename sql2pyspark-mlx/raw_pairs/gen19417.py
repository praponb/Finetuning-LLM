from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code").orderBy(F.col("starts_on").desc(), "id", "promo_id")
w2 = Window.orderBy("ends_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("row_num", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel")
w5 = Window.partitionBy("channel").orderBy("discount_pct", "id", "promo_id")
w6 = Window.partitionBy("promo_code").orderBy(F.col("promotion_discount_pct").desc())
w7 = Window.partitionBy("promotion_promo_code").orderBy("running_discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

matched = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

ranked = (
    matched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "row_num",
        F.first("promotion_discount_pct", ignorenulls=True).over(w2).alias("first_known_promotion_discount_pct"),
        F.sum("discount_pct").over(w3).alias("running_discount_pct"),
        F.count("promo_id").over(w4).alias("channel_count_promo_id"),
        F.lead("promotion_discount_pct", 1, 0).over(w5).alias("next_promotion_discount_pct"),
        F.cume_dist().over(w6).alias("promotion_discount_pct_cume_dist"),
    )
)

metrics_promotions = (
    metrics
    .select(
        "id",
        "ends_on",
        "promo_id",
        "promotion_promo_code",
        "first_known_promotion_discount_pct",
        "next_promotion_discount_pct",
        F.max("first_known_promotion_discount_pct").over(w7).alias("running_max_first_known_promotion_discount_pct"),
        F.sum("first_known_promotion_discount_pct").over(w7).alias("running_sum_first_known_promotion_discount_pct"),
        F.min("first_known_promotion_discount_pct").over(w7).alias("running_min_first_known_promotion_discount_pct"),
        F.avg("first_known_promotion_discount_pct").over(w7).alias("running_avg_first_known_promotion_discount_pct"),
    )
)

result = (
    metrics_promotions
    .groupBy("promo_id")
    .agg(
        F.count_if(F.col("promotion_promo_code") == "WELCOME5").alias("welcome5_events"),
        F.countDistinct("promotion_promo_code").alias("distinct_promotion_promo_codes"),
        F.count(F.when(F.col("promotion_promo_code") == "SPRING10", 1)).alias("spring10_rows"),
    )
)
