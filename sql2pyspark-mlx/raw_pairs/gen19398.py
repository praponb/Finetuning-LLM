from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("weeks_ends_on_to_starts_on")

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.when(F.col("discount_pct") >= 0.34, "High").when(F.col("discount_pct") >= 0.13, "Medium").otherwise("Low").alias("discount_pct_band"),
    )
)

matched_keys = (
    filtered
    .select("promo_code", "id")
    .intersectAll(
        df_promotions
        .filter(F.col("promo_code") == "BUNDLE")
        .select("promo_code", "id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("ends_on") > F.lit("2023-09-17").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct_band",
        (F.datediff("starts_on", "ends_on") / 7).alias("weeks_ends_on_to_starts_on"),
        F.coalesce("channel", F.lit("unassigned")).alias("channel_filled"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

valid = (
    cleaned
    .filter(F.col("starts_on") > F.lit("2023-09-03").cast("date"))
    .select("*", F.initcap(F.trim("channel")).alias("channel_title"))
)

with_windows = (
    valid
    .select(
        "ends_on",
        "discount_pct_band",
        "channel_filled",
        "channel_clean",
        "channel_title",
        F.dense_rank().over(w).alias("weeks_ends_on_to_starts_on_dense_rank"),
    )
)

result = (
    with_windows
    .select("discount_pct_band", "channel_filled", "channel_clean", "channel_title")
)
