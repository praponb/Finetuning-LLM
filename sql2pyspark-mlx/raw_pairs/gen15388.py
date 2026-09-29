from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("channel").orderBy("starts_on", "id")
w_grp = Window.partitionBy("promo_code", "channel").orderBy("prev_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("prev_discount_pct").desc())
w4 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("discount_pct")
w6 = Window.partitionBy("promo_code")
w7 = Window.partitionBy(F.year("ends_on"), "promo_code")
w8 = Window.partitionBy(F.month("ends_on"), "promo_code")

scored = (
    df_promotions
    .filter(F.col("promo_code").isin(["WELCOME5", "BUNDLE"]))
    .select(
        "*",
        F.lag("discount_pct").over(w_part).alias("prev_discount_pct"),
        F.lead("discount_pct").over(w_part).alias("next_discount_pct"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "next_discount_pct",
        "event_seq",
        F.last("next_discount_pct", ignorenulls=True).over(w_grp).alias("last_known_next_discount_pct"),
        F.rank().over(w3).alias("prev_discount_pct_rank"),
        F.first("next_discount_pct", ignorenulls=True).over(w4).alias("first_known_next_discount_pct"),
        F.cume_dist().over(w5).alias("discount_pct_cume_dist"),
    )
)

remaining_keys = (
    with_windows
    .select("channel", "promo_code")
    .subtract(
        df_promotions
        .filter(F.year("ends_on") == 2024)
        .select("channel", "promo_code")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "id",
        "discount_pct",
        "next_discount_pct",
        "event_seq",
        "prev_discount_pct_rank",
        F.sum("first_known_next_discount_pct").over(w6).alias("promo_code_sum_first_known_next_discount_pct"),
        F.cume_dist().over(w5).alias("discount_pct_cume_dist_2"),
        F.stddev_samp("discount_pct_cume_dist").over(w7).alias("ends_on_year_stddev_samp_discount_pct_cume_dist"),
        F.round((F.col("next_discount_pct") / F.sum("next_discount_pct").over(w6)), 4).alias("next_discount_pct_share"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w8)), 4).alias("discount_pct_share"),
    )
)

remaining_keys_2 = (
    metrics
    .select("id")
    .intersectAll(
        df_promotions
        .filter(F.month("starts_on").isin([1, 5, 8]))
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .cube(
        F.when(F.col("ends_on_year_stddev_samp_discount_pct_cume_dist") >= 1, "HIGH").otherwise("LOW").alias("ends_on_year_stddev_samp_discount_pct_cume_dist_tier"),
        F.when(F.col("promo_code_sum_first_known_next_discount_pct") >= 1, "LARGE").otherwise("SMALL").alias("promo_code_sum_first_known_next_discount_pct_tier"),
        F.when(F.col("prev_discount_pct_rank") >= 9, "LARGE").otherwise("SMALL").alias("prev_discount_pct_rank_tier"),
    )
    .agg(
        F.percentile_approx("next_discount_pct", 0.5).alias("next_discount_pct_p50"),
        F.percentile_approx("ends_on_year_stddev_samp_discount_pct_cume_dist", [0.25, 0.5, 0.75]).alias("ends_on_year_stddev_samp_discount_pct_cume_dist_quartiles"),
    )
)
