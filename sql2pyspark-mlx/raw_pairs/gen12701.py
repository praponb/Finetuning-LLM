from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first())
w3 = Window.partitionBy("city")
w_grp = Window.partitionBy("city").orderBy("rating_avg_5_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("rating_dense_rank", "id")

base = (
    df_suppliers
    .filter(F.col("supplier_name").like("P%"))
    .select(
        "*",
        F.when(F.col("rating") >= 3.5, "High").when(F.col("rating") >= 2.3, "Medium").otherwise("Low").alias("rating_level"),
    )
)

enriched = (
    base
    .filter(F.col("rating") >= 2.4)
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.avg("rating").over(w).alias("rating_avg_5"),
        F.dense_rank().over(w2).alias("rating_dense_rank"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
    )
)

filtered = (
    enriched
    .filter(F.col("country") != "UK")
    .select(
        "id",
        "city",
        "rating",
        "rating_avg_5",
        "rating_dense_rank",
        F.round((F.col("rating_avg_5") * 0.85), 2).alias("rating_avg_5_adjusted"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

with_windows = (
    filtered
    .withColumn("cumulative_rating", F.sum("rating").over(w_grp))
    .withColumn("rating_dense_rank_bucket", F.ntile(5).over(w5))
    .withColumn("city_avg_rating_dense_rank", F.avg("rating_dense_rank").over(w3))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("rating") >= 3.7, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
        F.when(F.col("rating_dense_rank_bucket") >= 7, "HIGH").otherwise("LOW").alias("rating_dense_rank_bucket_tier"),
        "city",
    )
    .agg(
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events_2"),
    )
    .orderBy("rating_tier")
)
