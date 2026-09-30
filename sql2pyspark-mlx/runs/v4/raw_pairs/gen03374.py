from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("country")
w_ordered = Window.partitionBy("country", "city")
w4 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last())

scored = (
    df_suppliers
    .withColumn("cumulative_rating", F.sum("rating").over(w_hist))
    .withColumn("country_stddev_samp_rating", F.stddev_samp("rating").over(w_running))
    .withColumn("country_sum_rating", F.sum("rating").over(w_ordered))
    .withColumn("rating_cume_dist", F.cume_dist().over(w4))
)

result = (
    scored
    .groupBy(
        F.when(F.col("cumulative_rating") >= 18, "HIGH").otherwise("LOW").alias("cumulative_rating_tier"),
        "country",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
        F.min("country_stddev_samp_rating").alias("min_country_stddev_samp_rating"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.round(F.sum("country_sum_rating") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("country_sum_rating_per_singapore"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
    )
)
