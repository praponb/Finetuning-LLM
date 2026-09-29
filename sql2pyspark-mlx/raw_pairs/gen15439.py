from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("val")
w2 = Window.orderBy(F.col("val_dense_rank_times_val").desc())

exploded = df_price_tiers.select("id", F.explode("tier_limits").alias("val"))

enriched = exploded.select("*", F.dense_rank().over(w).alias("val_dense_rank"))

cleaned = (
    enriched
    .filter(F.col("val_dense_rank") >= 4)
    .withColumn("val_dense_rank_times_val", (F.col("val_dense_rank") * F.col("val")))
)

metrics = (
    cleaned
    .select(
        "val",
        "val_dense_rank_times_val",
        F.dense_rank().over(w2).alias("val_dense_rank_times_val_dense_rank"),
        F.rank().over(w).alias("val_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("val_dense_rank_times_val_dense_rank") == 8)
    .withColumn("greatest_val_dense_rank_times_val_val", F.greatest("val_dense_rank_times_val", "val"))
    .withColumn("val_dense_rank_times_val_filled", F.coalesce("val_dense_rank_times_val", F.lit(0)))
)

base = (
    valid
    .filter(F.col("greatest_val_dense_rank_times_val_val") > 23)
    .withColumn("val_rank_num", F.col("val_rank").cast("string"))
)

stats = (
    base
    .groupBy(
        F.when(F.col("val_dense_rank_times_val") >= 84, "LARGE").otherwise("SMALL").alias("val_dense_rank_times_val_tier"),
    )
    .agg(
        F.percentile_approx("val_dense_rank_times_val_filled", 0.5).alias("val_dense_rank_times_val_filled_p50"),
        F.round(F.avg("val_dense_rank_times_val_filled"), 2).alias("avg_val_dense_rank_times_val_filled"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
    )
)

result = (
    stats
    .select("val_dense_rank_times_val_tier")
    .orderBy(F.col("val_dense_rank_times_val_tier").desc_nulls_last())
)
