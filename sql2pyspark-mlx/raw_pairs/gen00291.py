from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("days_ends_on_to_starts_on").asc_nulls_last(), "id")
w2 = Window.partitionBy("promotion_promo_code", "channel").orderBy(F.col("promotion_discount_pct").desc_nulls_first(), "id", "promo_id")

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & F.month("starts_on").isin([8, 9, 12]))
    .select("*", F.datediff("starts_on", "ends_on").alias("days_ends_on_to_starts_on"))
)

latest = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_window = (
    latest
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

base = (
    with_window
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-10-30"))) & F.col("promotion_promo_code").isNotNull())
    .withColumn("greatest_days_ends_on_to_starts_on_promo_id", F.greatest("days_ends_on_to_starts_on", "promo_id"))
    .withColumn("days_ends_on_to_starts_on_whole", F.floor("days_ends_on_to_starts_on"))
    .withColumn("resolved_promotion_promo_code", F.coalesce("promotion_promo_code", F.lit("NONE")))
)

latest_promotions = (
    base
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_promotions
    .select(
        "id",
        "promo_id",
        "promotion_promo_code",
        "greatest_days_ends_on_to_starts_on_promo_id",
        "discount_pct",
        "promotion_discount_pct",
    )
    .unpivot(
        ids=["id", "promo_id", "promotion_promo_code"],
        values=["greatest_days_ends_on_to_starts_on_promo_id", "discount_pct", "promotion_discount_pct"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
