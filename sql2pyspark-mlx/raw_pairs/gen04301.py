from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code").orderBy("discount_pct")
w2 = Window.partitionBy("promotion_promo_code").orderBy(F.col("promotion_discount_pct").desc(), "id", "promo_id")
w3 = Window.partitionBy("channel").orderBy("ends_on", "id", "promo_id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("channel")

matched = (
    df_promotions
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

deduped = (
    matched
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped_promotions = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_id",
    )
)

scored = (
    deduped_promotions
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "promo_id",
        F.min("promo_id").over(w3).alias("last3_min_promo_id"),
        F.sum("promo_id").over(w3).alias("last3_sum_promo_id"),
        F.avg("promo_id").over(w3).alias("last3_avg_promo_id"),
        F.round((F.col("promo_id") / F.sum("promo_id").over(w4)), 4).alias("promo_id_share"),
    )
)

result = (
    scored
    .groupBy("promo_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").desc_nulls_first())
)
