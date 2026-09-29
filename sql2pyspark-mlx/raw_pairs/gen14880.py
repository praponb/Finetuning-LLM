from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("ends_on").desc())
w2 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w4 = Window.orderBy(F.col("discount_pct").desc())

base = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2023-07-31").cast("date"))
    .select(
        "*",
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
        F.substring("channel", 1, 4).alias("channel_prefix"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

latest = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "promo_code", "discount_pct", "channel_prefix")
)

scored = (
    latest
    .select(
        "*",
        F.sum("discount_pct").over(w2).alias("cumulative_discount_pct"),
        F.lag("discount_pct", 1, 0).over(w3).alias("prev_discount_pct"),
        F.dense_rank().over(w4).alias("discount_pct_dense_rank"),
    )
)

remaining_keys = (
    scored
    .select("id", "promo_code")
    .subtract(
        df_promotions
        .filter(F.col("ends_on") > "2024-10-04")
        .select("id", "promo_code")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    verified
    .select("promo_code")
    .subtract(
        df_promotions
        .filter(F.col("starts_on") <= "2025-10-27")
        .select("promo_code")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

summary = (
    reconciled
    .groupBy(
        F.when(F.col("discount_pct") >= 0.32, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.round(F.sum("discount_pct_dense_rank") / F.nullif(F.count(F.when(F.col("promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("discount_pct_dense_rank_per_welcome5"),
        F.count_if(F.col("promo_code") == "WELCOME5").alias("welcome5_events"),
    )
    .filter(F.col("welcome5_events") > 1)
)

result = summary.select("discount_pct_tier", "welcome5_events")
