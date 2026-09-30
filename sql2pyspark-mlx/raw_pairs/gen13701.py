from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("ends_on"), "promo_code").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")
w3 = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")

valid = (
    df_promotions
    .filter((~F.col("channel").eqNullSafe(F.lit("partner"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.upper("channel").alias("channel_upper"),
    )
)

scored = (
    valid
    .select(
        "id",
        "channel",
        "starts_on",
        "channel_upper",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

matched = (
    scored
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

attributed = (
    matched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

attributed_promotions = (
    attributed
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    attributed_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "channel_upper",
        "row_seq",
        "row_seq_2",
        "promo_id",
        "promo_code",
        "discount_pct",
    )
    .orderBy("id")
)
