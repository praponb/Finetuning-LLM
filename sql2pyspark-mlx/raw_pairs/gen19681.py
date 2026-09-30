from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy(F.col("sqft").asc_nulls_first())
w3 = Window.partitionBy("city", "region").orderBy("sqft", "id")
w4 = Window.partitionBy("region").orderBy("sqft", "id")
w5 = Window.partitionBy("city").orderBy("sqft_avg_2")
w6 = Window.orderBy("sqft_cume_dist_filled", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.orderBy("sqft_cume_dist_filled", "id")

enriched = (
    df_stores
    .select(
        "id",
        "city",
        F.avg("sqft").over(w).alias("sqft_avg_2"),
        F.cume_dist().over(w2).alias("sqft_cume_dist"),
        F.ntile(3).over(w3).alias("sqft_bucket"),
        F.lag("sqft", 1).over(w4).alias("prev_sqft"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "sqft_avg_2", "sqft_cume_dist", "sqft_bucket", "prev_sqft")
)

filtered = (
    ranked
    .filter(F.col("sqft_cume_dist") > 0.72)
    .select("id", F.coalesce("sqft_cume_dist", F.lit(0)).alias("sqft_cume_dist_filled"))
)

enriched_stores = (
    filtered
    .select(
        "*",
        F.avg("sqft_cume_dist_filled").over(w6).alias("sqft_cume_dist_filled_avg_5"),
        F.ntile(4).over(w7).alias("sqft_cume_dist_filled_bucket"),
    )
)

stats = (
    enriched_stores
    .groupBy(
        F.when(F.col("sqft_cume_dist_filled_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("sqft_cume_dist_filled_bucket_tier"),
    )
    .agg(
        F.percentile_approx("sqft_cume_dist_filled", 0.95).alias("sqft_cume_dist_filled_p95"),
        F.max("sqft_cume_dist_filled").alias("max_sqft_cume_dist_filled"),
    )
)

result = (
    stats
    .select(
        "sqft_cume_dist_filled_bucket_tier",
        "sqft_cume_dist_filled_p95",
        "max_sqft_cume_dist_filled",
    )
    .orderBy(F.col("sqft_cume_dist_filled_p95").desc())
)
