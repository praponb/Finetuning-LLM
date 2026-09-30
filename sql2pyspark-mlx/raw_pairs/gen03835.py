from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("channel").orderBy("ends_on", "id")
w_recent = Window.partitionBy("channel", "promotion_promo_code").orderBy("row_seq", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_promotions
    .select(
        "*",
        F.lag("discount_pct").over(w_grp).alias("prev_discount_pct"),
        F.lead("discount_pct").over(w_grp).alias("next_discount_pct"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

attributed = (
    metrics
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows = (
    attributed
    .filter(F.col("row_seq") < 7)
    .withColumn("running_sum_promo_id", F.sum("promo_id").over(w_recent))
    .withColumn("running_count_promo_id", F.count("promo_id").over(w_recent))
    .withColumn("running_max_promo_id", F.max("promo_id").over(w_recent))
)

with_window = (
    with_windows
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

matched_keys = (
    with_window
    .select("promo_code", "id")
    .subtract(
        df_promotions
        .filter(F.col("channel").isin(["store", "mobile"]))
        .select("promo_code", "id")
    )
)

reconciled = (
    with_window
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("promotion_promo_code")
    .pivot("promotion_promo_code_2", ["BUNDLE", "VIP", "WELCOME5"])
    .agg(F.avg("running_sum_promo_id"))
)
