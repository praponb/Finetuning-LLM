from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").desc())
w2 = Window.orderBy(F.col("val_dense_rank").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

scored = lines.select("id", "effective_from", F.dense_rank().over(w).alias("val_dense_rank"))

scored_price_tiers = scored.withColumn("val_dense_rank_cume_dist", F.cume_dist().over(w2))

valid = (
    scored_price_tiers
    .filter(F.col("effective_from") > F.lit("2024-02-02").cast("date"))
    .select(
        "*",
        F.last_day("effective_from").alias("effective_from_month_end"),
        (F.col("val_dense_rank_cume_dist") + F.col("val_dense_rank")).alias("val_dense_rank_cume_dist_plus_val_dense_rank"),
    )
)

base = (
    valid
    .filter((F.col("effective_from") > F.lit("2024-03-14").cast("date")) & F.col("val_dense_rank_cume_dist").between(0.39, 0.55))
    .select(
        "id",
        "effective_from",
        "val_dense_rank",
        "val_dense_rank_cume_dist",
        (F.col("val_dense_rank_cume_dist") + F.col("val_dense_rank_cume_dist_plus_val_dense_rank")).alias("val_dense_rank_cume_dist_plus_val_dense_rank_cume_dist_plus_val_dense_rank"),
        (F.col("val_dense_rank") - F.col("val_dense_rank_cume_dist_plus_val_dense_rank")).alias("val_dense_rank_minus_val_dense_rank_cume_dist_plus_val_dense_rank"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("val_dense_rank_cume_dist") >= 0.65, "LARGE").otherwise("SMALL").alias("val_dense_rank_cume_dist_tier"),
        F.date_trunc("WEEK", "effective_from").alias("week_start"),
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("val_dense_rank_cume_dist", 0.95).alias("val_dense_rank_cume_dist_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
