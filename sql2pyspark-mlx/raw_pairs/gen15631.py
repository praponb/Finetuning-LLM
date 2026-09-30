from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("discount_pct")
w4 = Window.partitionBy("promo_code").orderBy(F.col("latest_rank").desc())

matched = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics = (
    matched
    .select(
        "id",
        "promo_code",
        "discount_pct",
        "promo_id",
        F.avg("promotion_discount_pct").over(w).alias("promotion_discount_pct_avg_6"),
        F.last("promotion_promo_code", ignorenulls=True).over(w2).alias("last_known_promotion_promo_code"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

latest_promotions = (
    latest
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct_avg_6",
        "last_known_promotion_promo_code",
        "latest_rank",
    )
)

result = (
    latest_promotions
    .groupBy(
        F.when(F.col("promotion_discount_pct_avg_6") >= 0, "LARGE").otherwise("SMALL").alias("promotion_discount_pct_avg_6_tier"),
    )
    .agg(
        F.sum("discount_pct").alias("total_discount_pct"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.avg("discount_pct").alias("avg_discount_pct"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy("event_count")
)
