from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("rating", "id")
w4 = Window.partitionBy("city")
w5 = Window.orderBy("cumulative_rating", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy("rating_bucket", "id")

filtered = (
    df_suppliers
    .filter(F.col("city") != "New York")
    .withColumn("rating_band", F.when(F.col("rating") >= 4.5, "High").when(F.col("rating") >= 2.5, "Medium").otherwise("Low"))
    .withColumn("country_prefix", F.substring("country", 1, 4))
)

enriched = (
    filtered
    .select(
        "id",
        "city",
        "rating",
        "rating_band",
        F.sum("rating").over(w_hist).alias("running_rating"),
        F.sum("rating").over(w2).alias("cumulative_rating"),
        F.ntile(3).over(w3).alias("rating_bucket"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "city",
        "rating",
        "cumulative_rating",
        "rating_bucket",
        F.round((F.col("rating_bucket") / F.sum("rating_bucket").over(w4)), 4).alias("rating_bucket_share"),
        F.avg("rating_bucket").over(w5).alias("rating_bucket_avg_3"),
    )
)

with_windows = (
    metrics
    .filter(F.col("rating") <= 2.0)
    .select(
        "*",
        F.sum("rating_bucket_avg_3").over(w6).alias("running_rating_bucket_avg_3"),
        F.ntile(3).over(w7).alias("rating_bucket_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("running_rating_bucket_avg_3") >= 43, "HIGH").otherwise("LOW").alias("running_rating_bucket_avg_3_tier"),
    )
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("running_rating_bucket_avg_3"))).alias("tokyo_total_running_rating_bucket_avg_3"),
        F.round(F.sum("rating_bucket_avg_3") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("rating_bucket_avg_3_per_new_york"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("rating_bucket_avg_3_per_new_york") > 0)
    .orderBy(F.col("tokyo_total_running_rating_bucket_avg_3").asc_nulls_first())
)
