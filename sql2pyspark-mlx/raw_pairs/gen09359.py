from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promotion_promo_code_2").orderBy(F.col("promo_id_2").desc(), "id", "promo_id", "promo_id_2")
w3 = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id", "promo_id", "promo_id_2")

ranked = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "channel", "promo_code", "ends_on", "discount_pct")
)

matched = (
    ranked
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("e.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched_promotions = (
    matched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

filtered = (
    matched_promotions
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-05-23"))) & (F.col("promotion_promo_code") == "BUNDLE"))
    .withColumn("promo_code_upper", F.upper("promo_code"))
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        "promo_id_2",
        "promo_code_upper",
    )
)

ranked_promotions = (
    latest
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

base = (
    ranked_promotions
    .filter((~F.col("channel").eqNullSafe(F.lit("web"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        "promo_id_2",
        "promo_code_upper",
        (F.datediff(F.current_date(), "ends_on") / 7).alias("weeks_since_ends_on"),
    )
)

result = (
    base
    .select(
        "id",
        "promo_code",
        "ends_on",
        "promo_id",
        "promotion_discount_pct",
        "promo_id_2",
        "promo_code_upper",
    )
    .orderBy(F.col("promo_id_2").desc())
)
