from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Tokyo", "Bangkok"])
    .agg(F.sum("sqft").alias("total"), F.count(F.lit(1)).alias("n"))
)
