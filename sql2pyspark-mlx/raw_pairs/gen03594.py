from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("channel").orderBy("running_avg_discount_pct", "id")
w4 = Window.partitionBy("channel", "promo_code").orderBy("running_sum_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-10-13"))) & F.col("channel").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("promo_code")).alias("promo_code_title"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

metrics = (
    base
    .filter(F.col("discount_pct") > 0.13)
    .withColumn("running_sum_discount_pct", F.sum("discount_pct").over(w))
    .withColumn("running_min_discount_pct", F.min("discount_pct").over(w))
    .withColumn("running_avg_discount_pct", F.avg("discount_pct").over(w))
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w2))
)

scored = (
    metrics
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "running_avg_discount_pct",
        "discount_pct_percent_rank",
        F.ntile(5).over(w3).alias("running_avg_discount_pct_bucket"),
        F.row_number().over(w3).alias("seq_no"),
        F.first("running_min_discount_pct", ignorenulls=True).over(w4).alias("first_known_running_min_discount_pct"),
    )
)

attributed = (
    scored
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    attributed
    .groupBy(
        F.when(F.col("discount_pct_percent_rank") >= 0.76, "HIGH").otherwise("LOW").alias("discount_pct_percent_rank_tier"),
    )
    .agg(
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.percentile_approx("discount_pct_percent_rank", 0.95).alias("discount_pct_percent_rank_p95"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.round(F.avg("discount_pct_percent_rank"), 2).alias("avg_discount_pct_percent_rank"),
    )
)
