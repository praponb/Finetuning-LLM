from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("discount_pct").desc(), "id")
w3 = Window.partitionBy("promo_code")
w4 = Window.partitionBy("channel").orderBy(F.col("promo_id").desc())
w5 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id", "promo_id", "promo_id_2")
w6 = Window.partitionBy("channel").orderBy("promo_id_2", "id", "promo_id", "promo_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.year("ends_on"), "promo_code").orderBy("promotion_discount_pct", "id", "promo_id", "promo_id_2")
w8 = Window.orderBy("promo_id_2", "id", "promo_id", "promo_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_promotions
    .filter(F.year("starts_on") >= 2025)
    .select(
        "id",
        "channel",
        "ends_on",
        F.last("channel", ignorenulls=True).over(w).alias("last_known_channel"),
        F.ntile(3).over(w2).alias("discount_pct_bucket"),
        F.min("discount_pct").over(w3).alias("promo_code_min_discount_pct"),
    )
)

attributed = (
    metrics
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

matched = (
    attributed
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

latest = (
    matched
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "channel",
        "ends_on",
        "last_known_channel",
        "promo_code_min_discount_pct",
        "promo_id",
        "promo_code",
        "discount_pct",
        "promo_id_2",
        "promotion_discount_pct",
    )
)

result = (
    latest
    .select(
        "id",
        "channel",
        "ends_on",
        "last_known_channel",
        "promo_code_min_discount_pct",
        "promo_id",
        "promo_code",
        "promo_id_2",
        F.ntile(10).over(w5).alias("discount_pct_bucket"),
        F.sum("promo_id_2").over(w6).alias("running_promo_id_2"),
        F.lag("promo_id_2", 1).over(w7).alias("prev_promo_id_2"),
        F.last("promo_id_2", ignorenulls=True).over(w8).alias("last_known_promo_id_2"),
    )
)
