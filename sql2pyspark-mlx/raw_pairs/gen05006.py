from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("discount_pct", "id")
w3 = Window.partitionBy(F.date_trunc("month", "ends_on"), "promo_code").orderBy("discount_pct", "id")
w4 = Window.partitionBy("promotion_promo_code")
w5 = Window.orderBy(F.col("discount_pct").desc_nulls_first())
w6 = Window.orderBy(F.col("prev_discount_pct").asc_nulls_last())
w7 = Window.orderBy(F.col("discount_pct").desc_nulls_last())
w8 = Window.orderBy(F.col("discount_pct").desc())

enriched = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.sum("discount_pct").over(w).alias("cumulative_discount_pct"),
        F.lag("discount_pct", 1).over(w_grp).alias("prev_discount_pct"),
        F.lag("discount_pct", 1).over(w3).alias("prev_discount_pct_2"),
    )
)

kept_keys = (
    enriched
    .select("id")
    .exceptAll(
        df_promotions
        .filter(F.col("starts_on") > "2024-10-01")
        .select("id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_window = (
    kept_rows
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics = (
    with_window
    .filter(F.col("prev_discount_pct_2") <= 0.17)
    .select(
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "cumulative_discount_pct",
        "prev_discount_pct",
        "discount_pct",
        F.count("cumulative_discount_pct").over(w4).alias("promotion_promo_code_count_cumulative_discount_pct"),
        F.dense_rank().over(w5).alias("discount_pct_dense_rank"),
        F.percent_rank().over(w6).alias("prev_discount_pct_percent_rank"),
        F.percent_rank().over(w7).alias("discount_pct_percent_rank"),
        F.dense_rank().over(w8).alias("discount_pct_dense_rank_2"),
    )
)

with_window_promotions = (
    metrics
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    with_window_promotions
    .select(
        "promo_code",
        "ends_on",
        "cumulative_discount_pct",
        "discount_pct",
        "promotion_promo_code_count_cumulative_discount_pct",
        "discount_pct_dense_rank",
        "discount_pct_percent_rank",
        "promo_id",
        "promotion_promo_code",
    )
    .orderBy(F.col("cumulative_discount_pct").desc_nulls_first())
)
