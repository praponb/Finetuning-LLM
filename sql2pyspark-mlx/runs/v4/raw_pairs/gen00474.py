from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("rating_tier").orderBy(F.col("distinct_cities").desc())

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.6, "HIGH").otherwise("LOW").alias("rating_tier"),
        "city",
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.countDistinct("city").alias("distinct_cities"),
        F.percentile_approx("rating", 0.5).alias("rating_p50"),
    )
    .withColumn("distinct_cities_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("city").desc_nulls_first())
)
