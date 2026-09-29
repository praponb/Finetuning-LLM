from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w3 = Window.partitionBy("promotion_promo_code").orderBy(F.col("discount_pct").desc(), "id", "promo_id", "promo_id_2")

metrics = (
    df_promotions
    .filter(F.col("starts_on") <= "2023-03-07")
    .select(
        "*",
        F.min("discount_pct").over(w).alias("last3_min_discount_pct"),
        F.sum("discount_pct").over(w).alias("last3_sum_discount_pct"),
        F.count("discount_pct").over(w).alias("last3_count_discount_pct"),
        F.max("discount_pct").over(w).alias("last3_max_discount_pct"),
        F.ntile(3).over(w2).alias("discount_pct_bucket"),
    )
)

matched = (
    metrics
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched_promotions = (
    matched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

deduped = (
    matched_promotions
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    deduped
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-06-03"))) & F.col("promotion_promo_code").isNotNull())
    .withColumn("last3_min_discount_pct_filled", F.coalesce("last3_min_discount_pct", F.lit(0)))
)

with_window = (
    cleaned
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

result = (
    with_window
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "last3_min_discount_pct",
        "last3_sum_discount_pct",
        "discount_pct_bucket",
        F.when(F.col("promo_code").rlike("^(WELCOME5|VIP)$"), "GROUP_B").when(F.col("promo_code").rlike("^(SPRING10|BUNDLE)$"), "OTHER_ZONE").otherwise("CORE").alias("promo_code_zone"),
    )
    .orderBy(F.col("ends_on").desc_nulls_last(), "discount_pct")
)
