from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("promo_code").orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promotion_promo_code_2").orderBy(F.col("running_avg_promotion_discount_pct").asc_nulls_first(), "id", "promo_id", "promo_id_2")
w3 = Window.partitionBy("promotion_promo_code_2").orderBy(F.col("promo_id").desc_nulls_first(), "id", "promo_id", "promo_id_2")
w_recent = Window.partitionBy("promotion_promo_code_2").orderBy("discount_pct", "id", "promo_id", "promo_id_2")

attributed = (
    df_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched = (
    attributed
    .filter(F.col("promo_id") == 8)
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "promo_id",
        "promotion_promo_code",
        F.min("promotion_discount_pct").over(w_running).alias("running_min_promotion_discount_pct"),
        F.sum("promotion_discount_pct").over(w_running).alias("running_sum_promotion_discount_pct"),
        F.max("promotion_discount_pct").over(w_running).alias("running_max_promotion_discount_pct"),
        F.avg("promotion_discount_pct").over(w_running).alias("running_avg_promotion_discount_pct"),
    )
)

matched = (
    enriched
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("e.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct"),
    )
)

cleaned = (
    matched
    .filter(~F.col("promotion_promo_code_2").eqNullSafe(F.lit("SPRING10")))
    .select(
        "id",
        "promo_id",
        "running_sum_promotion_discount_pct",
        "running_avg_promotion_discount_pct",
        "promo_id_2",
        "promotion_promo_code_2",
        "discount_pct",
        F.when(F.col("running_avg_promotion_discount_pct") >= 0.34, "High").when(F.col("running_avg_promotion_discount_pct") >= 0.18, "Medium").otherwise("Low").alias("running_avg_promotion_discount_pct_level"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped_promotions = (
    deduped
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "promo_id",
        "running_sum_promotion_discount_pct",
        "promo_id_2",
        "promotion_promo_code_2",
        "discount_pct",
    )
)

result = (
    deduped_promotions
    .select(
        "id",
        "promo_id",
        "promo_id_2",
        "promotion_promo_code_2",
        F.lag("discount_pct").over(w_recent).alias("prev_discount_pct"),
        F.lead("promo_id").over(w_recent).alias("next_promo_id"),
        F.lag("promo_id").over(w_recent).alias("prev_promo_id"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)
