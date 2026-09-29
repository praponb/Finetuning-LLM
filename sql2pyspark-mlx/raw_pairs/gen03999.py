from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region", "city")
w4 = Window.partitionBy("region", "city").orderBy("sqft", "id")
w5 = Window.partitionBy("region").orderBy(F.col("sqft").desc())
w6 = Window.partitionBy("city", "region").orderBy(F.col("sqft_cume_dist").desc())
w7 = Window.orderBy("sqft_cume_dist", "id")
w8 = Window.partitionBy(F.date_trunc("WEEK", "opened_date")).orderBy("sqft_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy(F.month("opened_date")).orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("sqft_cume_dist").desc_nulls_last())

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-09-13").cast("date")) & (F.col("region").eqNullSafe(F.lit("South"))))
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM-dd"))
)

scored = (
    valid
    .select(
        "*",
        F.ntile(3).over(w).alias("sqft_bucket"),
        F.last("sqft", ignorenulls=True).over(w2).alias("last_known_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
        F.ntile(10).over(w4).alias("sqft_bucket_2"),
        F.cume_dist().over(w5).alias("sqft_cume_dist"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.rank().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "opened_date", "sqft_cume_dist")
)

enriched = (
    deduped
    .filter(F.col("sqft_cume_dist").between(0.45, 0.75))
    .select(
        "*",
        F.lag("sqft_cume_dist", 1).over(w7).alias("prev_sqft_cume_dist"),
        F.sum("sqft_cume_dist").over(w8).alias("cumulative_sqft_cume_dist"),
        F.first("sqft_cume_dist", ignorenulls=True).over(w9).alias("first_known_sqft_cume_dist"),
        F.percent_rank().over(w10).alias("sqft_cume_dist_percent_rank"),
    )
)

stats = (
    enriched
    .cube(
        F.when(F.col("prev_sqft_cume_dist") >= 0.68, "HIGH").otherwise("LOW").alias("prev_sqft_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("prev_sqft_cume_dist", 0.5).alias("prev_sqft_cume_dist_p50"),
        F.round(F.avg("prev_sqft_cume_dist"), 2).alias("avg_prev_sqft_cume_dist"),
    )
    .filter(F.col("prev_sqft_cume_dist_p50") > 0)
)

result = (
    stats
    .select("prev_sqft_cume_dist_p50")
    .orderBy(F.col("prev_sqft_cume_dist_p50").desc())
)
