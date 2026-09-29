from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("months_starts_on_to_ends_on").desc(), "id", "promo_id", "promo_id_2")
w2 = Window.partitionBy("promotion_promo_code", "promotion_promo_code_2").orderBy(F.col("promotion_discount_pct_2").desc(), "id", "promo_id", "promo_id_2")

base = (
    df_promotions
    .filter(F.col("channel") == "store")
    .withColumn("months_starts_on_to_ends_on", F.round((F.datediff("ends_on", "starts_on") / 30), 1))
)

with_window = (
    base
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

cleaned = (
    with_window
    .filter(F.col("channel").isNotNull())
    .withColumn("promotion_discount_pct_per_months_starts_on_to_ends_on", (F.col("promotion_discount_pct") / F.nullif("months_starts_on_to_ends_on", F.lit(0))))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("UNKNOWN")))
)

matched = (
    cleaned
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("e.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

deduped = (
    matched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "promo_id",
        "promo_id_2",
        "promotion_promo_code",
        "promotion_discount_pct_2",
        "promotion_discount_pct",
        "promotion_discount_pct_per_months_starts_on_to_ends_on",
    )
    .unpivot(
        ids=["id", "promo_id", "promo_id_2", "promotion_promo_code"],
        values=["promotion_discount_pct_2", "promotion_discount_pct", "promotion_discount_pct_per_months_starts_on_to_ends_on"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
