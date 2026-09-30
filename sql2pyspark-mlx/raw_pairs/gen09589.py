from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("region").orderBy("opened_date", "id")
w3 = Window.partitionBy(F.month("opened_date")).orderBy("opened_date", "id").rowsBetween(-3, -1)

enriched = (
    df_stores
    .select(
        "*",
        F.ntile(3).over(w).alias("sqft_bucket"),
        F.lead("sqft", 1).over(w2).alias("next_sqft"),
        F.avg("sqft").over(w3).alias("sqft_avg_3_prior"),
    )
)

result = (
    enriched
    .cube(
        "city",
        F.when(F.col("sqft_bucket") >= 8, "HIGH").otherwise("LOW").alias("sqft_bucket_tier"),
    )
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("sqft_bucket", [0.25, 0.5, 0.75]).alias("sqft_bucket_quartiles"),
        F.avg(F.when(F.col("region") == "East", F.col("sqft_bucket"))).alias("east_avg_sqft_bucket"),
        F.count_if(F.col("region") == "North").alias("north_events"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_rows"),
    )
    .filter(F.col("east_avg_sqft_bucket") > 0)
    .orderBy("north_events")
)
