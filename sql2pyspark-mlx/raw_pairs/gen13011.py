from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")

base = (
    df_promotions
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-12-12"))) & F.col("channel").isNotNull())
    .withColumn("weeks_starts_on_to_ends_on", (F.datediff("ends_on", "starts_on") / 7))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

valid = (
    ranked
    .filter(~F.col("channel").eqNullSafe(F.lit("store")))
    .select(
        "*",
        F.upper("promo_code").alias("promo_code_upper"),
        F.round("discount_pct", 1).alias("discount_pct_rounded"),
    )
)

matched_keys = (
    valid
    .select("channel", "promo_code")
    .subtract(
        df_promotions
        .filter(F.col("channel").isin(["mobile", "store"]))
        .select("channel", "promo_code")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

attributed = (
    kept_rows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    attributed
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "weeks_starts_on_to_ends_on",
        "resolved_channel",
        "promo_code_upper",
        "discount_pct_rounded",
    )
    .orderBy(F.col("id").desc(), "ends_on")
)
