from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promotion_promo_code").orderBy(F.col("discount_pct").desc(), "id", "promo_id")
w2 = Window.partitionBy("promotion_promo_code").orderBy("promo_id", "id", "promo_id")
w3 = Window.partitionBy("promotion_promo_code").orderBy("discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promotion_promo_code").orderBy(F.col("event_seq").desc(), "id", "promo_id")
w5 = Window.partitionBy("promotion_promo_code").orderBy(F.col("starts_on").desc())

attributed = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

deduped = (
    attributed
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "starts_on",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
    )
)

with_windows = (
    deduped
    .filter(F.col("promo_id") > 4)
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("cumulative_promotion_discount_pct", F.sum("promotion_discount_pct").over(w3))
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

latest_promotions = (
    latest
    .withColumn("row_num_2", F.rank().over(w5))
    .filter(F.col("row_num_2") == 1)
)

result = (
    latest_promotions
    .groupBy("promo_id")
    .agg(
        F.count_if(F.col("promotion_promo_code") == "BUNDLE").alias("bundle_events"),
        F.count(F.when(F.col("promotion_promo_code") == "SPRING10", 1)).alias("spring10_rows"),
        F.count(F.when(F.col("promotion_promo_code") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.sum(F.when(F.col("promotion_promo_code") == "VIP", F.col("discount_pct"))).alias("vip_total_discount_pct"),
    )
    .orderBy(F.col("bundle_events").asc_nulls_first())
)
