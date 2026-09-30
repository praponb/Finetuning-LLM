from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft", "id")
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("city", "region")
w4 = Window.partitionBy("city").orderBy("opened_date", "id")
w5 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last())

scored = (
    df_stores
    .select(
        "*",
        F.ntile(4).over(w).alias("sqft_bucket"),
        F.ntile(4).over(w2).alias("sqft_bucket_2"),
        F.avg("sqft").over(w3).alias("city_avg_sqft"),
        F.row_number().over(w4).alias("row_seq"),
        F.cume_dist().over(w5).alias("sqft_cume_dist"),
    )
)

result = (
    scored
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_rows"),
        F.sum(F.when(F.col("region") == "Central", F.col("sqft_cume_dist"))).alias("central_total_sqft_cume_dist"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.countDistinct("city").alias("distinct_cities"),
        F.round(F.sum("city_avg_sqft") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("city_avg_sqft_per_london"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
    )
    .filter(F.col("distinct_cities") > 1)
    .orderBy(F.col("week_start").desc_nulls_last())
)
