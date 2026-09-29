from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("num_cities").desc())

result = (
    df_stores
    .groupBy(
        "region",
        F.when(F.col("sqft") >= 18035, "HIGH").otherwise("LOW").alias("sqft_tier"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "South", 1)), F.lit(0)), 2).alias("sqft_per_south"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.min("sqft").alias("min_sqft"),
    )
    .withColumn("num_cities_dense_rank", F.dense_rank().over(w))
)
