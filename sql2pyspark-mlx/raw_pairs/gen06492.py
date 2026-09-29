from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id", "promo_id")
w2 = Window.orderBy(F.col("promo_id_2").desc_nulls_first())
w3 = Window.orderBy(F.col("promo_id_2").desc())

attributed = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

latest = (
    attributed
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    latest
    .filter((F.col("ends_on") >= F.lit("2023-06-24").cast("date")) & F.col("promotion_promo_code").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_id",
        "row_num",
        F.upper("channel").alias("channel_upper"),
        F.coalesce("promotion_promo_code", F.lit("unassigned")).alias("promotion_promo_code_filled"),
    )
)

attributed_promotions = (
    cleaned
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

matched_keys = (
    attributed_promotions
    .select("promo_code")
    .subtract(
        df_promotions
        .filter(F.col("channel").isin(["web", "mobile", "store"]))
        .select("promo_code")
    )
)

reconciled = (
    attributed_promotions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "channel_upper",
        "promotion_promo_code_filled",
        "promo_id_2",
        F.percent_rank().over(w2).alias("promo_id_2_percent_rank"),
        F.rank().over(w3).alias("promo_id_2_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "starts_on",
        "ends_on",
        "promotion_promo_code_filled",
        "promo_id_2",
        "promo_id_2_rank",
    )
    .orderBy(F.col("promo_id_2_rank").desc())
)
