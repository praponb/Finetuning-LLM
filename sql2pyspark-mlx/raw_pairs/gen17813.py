from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("promo_id").asc_nulls_last(), "id", "promo_id")
w2 = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id", "promo_id")
w3 = Window.partitionBy("promotion_promo_code_2")
w4 = Window.partitionBy(F.date_trunc("week", "starts_on"), "channel").orderBy(F.col("promotion_discount_pct").desc(), "id", "promo_id", "promo_id_2", "promo_id_3")
w5 = Window.partitionBy("promotion_promo_code", "promotion_promo_code_2").orderBy("promotion_discount_pct_3", "id", "promo_id", "promo_id_2", "promo_id_3")
w_hist = Window.orderBy(F.col("latest_rank_2").desc(), "id", "promo_id", "promo_id_2", "promo_id_3")

matched = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("s.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

latest = (
    matched
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    latest
    .withColumn("latest_rank_2", F.row_number().over(w2))
    .filter(F.col("latest_rank_2") == 1)
)

with_window = (
    deduped
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

attributed = (
    with_window
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

result = (
    attributed
    .filter(F.col("discount_pct") < 0.16)
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "latest_rank",
        "latest_rank_2",
        "promo_id_2",
        "promotion_promo_code_2",
        "promo_id_3",
        "promotion_discount_pct_3",
        F.round((F.col("promo_id") / F.sum("promo_id").over(w3)), 4).alias("promo_id_share"),
        F.row_number().over(w4).alias("event_seq"),
        F.lag("discount_pct", 2).over(w5).alias("prev_discount_pct"),
        F.round((F.col("promo_id") / F.sum("promo_id").over(w3)), 4).alias("promo_id_share_2"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)
