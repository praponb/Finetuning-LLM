from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc_nulls_last(), "id")

ranked = (
    df_stores
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("sqft") >= 16503, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.min("sqft").alias("min_sqft"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.percentile_approx("sqft", 0.5).alias("sqft_p50"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft_2"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft_3"),
    )
    .orderBy(F.col("avg_sqft_3").asc_nulls_last())
)
