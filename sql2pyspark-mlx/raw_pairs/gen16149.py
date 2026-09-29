from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("promo_id").desc_nulls_first(), "id", "promo_id", "promo_id_2")

filtered = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2023-10-12").cast("date"))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.when(F.col("promo_code") == "BUNDLE", 1).otherwise(0).alias("is_bundle"),
        F.add_months("starts_on", 12).alias("starts_on_plus_months"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

matched = (
    filtered
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.starts_on_plus_months") >= F.col("pr.starts_on")) & (F.col("a.starts_on_plus_months") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

cleaned = (
    matched
    .filter((F.col("ends_on") >= F.lit("2023-08-07").cast("date")) & (~F.col("promo_code").eqNullSafe(F.lit("BUNDLE"))))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "is_bundle",
        "starts_on_plus_months",
        "channel_clean",
        "promo_id",
        "promo_code",
        "promotion_discount_pct",
        F.when(F.col("promo_code") == "BUNDLE", 1).otherwise(0).alias("is_bundle_2"),
        F.lower("promo_code").alias("promo_code_lower"),
    )
)

with_window = (
    cleaned
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

cleaned_promotions = (
    with_window
    .filter(F.col("channel").isin(["partner", "web", "mobile"]) & (F.col("channel").eqNullSafe(F.lit("web"))))
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        "promo_id",
        "promo_code",
        "promo_code_lower",
        "promo_id_2",
        "promotion_promo_code",
        "promotion_discount_pct_2",
        F.year("starts_on_plus_months").alias("starts_on_plus_months_year"),
    )
)

deduped = (
    cleaned_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "promo_id",
        "promo_id_2",
        "promotion_promo_code",
        "promotion_discount_pct_2",
        "discount_pct",
    )
    .unpivot(
        ids=["id", "promo_id", "promo_id_2", "promotion_promo_code"],
        values=["promotion_discount_pct_2", "discount_pct"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
