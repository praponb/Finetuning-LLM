from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("channel", "promo_code")
w3 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc(), "id")
w4 = Window.partitionBy("channel", "promo_code").orderBy(F.col("ends_on").desc(), "id")

base = (
    df_promotions
    .filter((F.col("discount_pct") >= 0.12) & F.col("channel").isNotNull())
    .withColumn("discount_pct_whole", F.floor("discount_pct"))
)

metrics = (
    base
    .filter(F.col("discount_pct") >= 0.25)
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.sum("discount_pct_whole").over(w2).alias("channel_sum_discount_pct_whole"),
        F.row_number().over(w3).alias("event_seq"),
        F.lead("discount_pct_whole", 1).over(w).alias("next_discount_pct_whole"),
    )
)

latest = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

with_window = (
    latest
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    with_window
    .groupBy("promo_id")
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.round(F.sum("channel_sum_discount_pct_whole") / F.nullif(F.count(F.when(F.col("promo_code") == "SPRING10", 1)), F.lit(0)), 2).alias("channel_sum_discount_pct_whole_per_spring10"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("channel")).alias("num_channels_2"),
        F.count_if(F.col("promo_code") == "SPRING10").alias("spring10_events"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("num_channels_2").desc_nulls_first())
)
