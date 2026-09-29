from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promotion_promo_code").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("channel", "promotion_promo_code").orderBy("starts_on", "id", "promo_id")
w4 = Window.orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("discount_pct").desc(), "id", "promo_id", "promo_id_2")
w6 = Window.partitionBy("promotion_promo_code").orderBy("promo_id_2", "id", "promo_id", "promo_id_2").rowsBetween(-3, -1)
w7 = Window.partitionBy("channel").orderBy("ends_on", "id", "promo_id", "promo_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("promotion_promo_code").orderBy(F.col("ends_on").desc(), "id", "promo_id", "promo_id_2")
w9 = Window.partitionBy("channel").orderBy(F.col("promo_id").desc_nulls_last())

deduped = (
    df_promotions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_window = (
    deduped
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows = (
    with_window
    .filter(F.col("starts_on") >= "2024-02-10")
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("running_promo_id", F.sum("promo_id").over(w4))
)

matched = (
    with_windows
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

metrics = (
    matched
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        "promo_id_2",
        "promotion_discount_pct_2",
        F.row_number().over(w5).alias("seq_no"),
        F.avg("promotion_discount_pct_2").over(w6).alias("promotion_discount_pct_2_avg_3_prior"),
        F.sum("discount_pct_percent_rank").over(w7).alias("cumulative_discount_pct_percent_rank"),
        F.row_number().over(w8).alias("event_seq"),
        F.cume_dist().over(w9).alias("promo_id_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "promo_code",
        "ends_on",
        "promo_id",
        "promotion_discount_pct",
        "promo_id_2",
        "promotion_discount_pct_2",
        "seq_no",
        "promotion_discount_pct_2_avg_3_prior",
        F.when(F.col("promo_code").rlike("^(SPRING10|BUNDLE)$"), "PRIORITY").otherwise("GROUP_A").alias("promo_code_zone"),
    )
)
