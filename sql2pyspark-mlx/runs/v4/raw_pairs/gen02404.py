from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct")
w2 = Window.orderBy("days_since_ends_on_2", "id")
w3 = Window.partitionBy("channel").orderBy("days_since_ends_on_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("days_since_ends_on_2", "id", "promo_id", "promo_id_2", "promo_id_3")
w5 = Window.partitionBy("promo_code").orderBy(F.col("promo_id_3").asc_nulls_first())
w_ordered = Window.partitionBy("promotion_promo_code").orderBy(F.col("running_discount_pct").desc_nulls_last())
w7 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_percent_rank").desc_nulls_last())
w8 = Window.partitionBy("promotion_promo_code").orderBy(F.col("next_days_since_ends_on_2").desc_nulls_last())

filtered = (
    df_promotions
    .filter(F.col("promo_code") == "SPRING10")
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on_2"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.percent_rank().over(w).alias("discount_pct_percent_rank"),
        F.lag("days_since_ends_on", 1).over(w2).alias("prev_days_since_ends_on"),
        F.sum("discount_pct").over(w3).alias("running_discount_pct"),
    )
)

with_window = (
    metrics
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

attributed = (
    with_window
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

matched = (
    attributed
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

with_windows = (
    matched
    .select(
        "*",
        F.lead("days_since_ends_on_2", 1).over(w_recent).alias("next_days_since_ends_on_2"),
        F.percent_rank().over(w5).alias("promo_id_3_percent_rank"),
        F.cume_dist().over(w_ordered).alias("running_discount_pct_cume_dist"),
    )
)

kept_keys = (
    with_windows
    .select("channel")
    .subtract(
        df_promotions
        .filter(F.col("ends_on") <= "2023-03-04")
        .select("channel")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("discount_pct_percent_rank_rank", F.rank().over(w7))
    .withColumn("next_days_since_ends_on_2_percent_rank", F.percent_rank().over(w8))
)
