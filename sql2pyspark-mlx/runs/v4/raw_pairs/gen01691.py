from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last())

ranked = (
    df_stores
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("sqft") >= 16618, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.countDistinct("city").alias("distinct_cities"),
        F.sum(F.when(F.col("region") == "Central", F.col("sqft")).otherwise(0)).alias("central_sqft"),
    )
    .orderBy(F.col("sqft_tier").desc_nulls_last())
)
