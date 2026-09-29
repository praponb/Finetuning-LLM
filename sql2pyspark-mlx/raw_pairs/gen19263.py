from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w_recent = Window.partitionBy("promo_code")
w3 = Window.partitionBy("promo_code").orderBy("starts_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("promotion_discount_pct", "id", "promo_id")
w5 = Window.partitionBy("promo_code").orderBy(F.col("row_seq").desc(), "id", "promo_id")

latest = (
    df_promotions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "channel", "starts_on", "discount_pct")
)

matched = (
    latest
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics = (
    matched
    .select(
        "id",
        "starts_on",
        "discount_pct",
        "promo_id",
        "promo_code",
        F.count("promotion_discount_pct").over(w_recent).alias("promo_code_count_promotion_discount_pct"),
        F.last("channel", ignorenulls=True).over(w3).alias("last_known_channel"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy("promo_code")
    .agg(
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_rows"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "VIP", 1)), F.lit(0)), 2).alias("discount_pct_per_vip"),
    )
    .filter(F.col("discount_pct_per_vip") > 0)
)
