from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id")
w2 = Window.partitionBy("promo_code", "channel")
w3 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w4 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("next_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("promo_code").orderBy(F.col("running_avg_next_discount_pct").desc_nulls_first(), "id")

enriched = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.row_number().over(w).alias("row_seq"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
        F.lead("discount_pct", 1).over(w3).alias("next_discount_pct"),
    )
)

enriched_promotions = (
    enriched
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "next_discount_pct",
        F.min("next_discount_pct").over(w4).alias("running_min_next_discount_pct"),
        F.sum("next_discount_pct").over(w4).alias("running_sum_next_discount_pct"),
        F.avg("next_discount_pct").over(w4).alias("running_avg_next_discount_pct"),
        F.first("row_seq", ignorenulls=True).over(w5).alias("first_known_row_seq"),
    )
)

ranked = (
    enriched_promotions
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

matched = (
    ranked
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

attributed = (
    matched
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    attributed
    .select(
        "id",
        "next_discount_pct",
        "running_avg_next_discount_pct",
        "first_known_row_seq",
        "latest_rank",
        "promo_id",
        "promotion_promo_code",
        "promo_id_2",
        "promotion_discount_pct",
    )
    .orderBy("promotion_promo_code")
)
