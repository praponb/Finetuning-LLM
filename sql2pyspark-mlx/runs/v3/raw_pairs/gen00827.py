from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.when(F.col("sqft") >= 13305, "high").when(F.col("sqft") >= 7860, "mid").otherwise("low").alias("sqft_bucket"))
    .pivot("city", ["Singapore", "Tokyo", "New York", "Chiang Mai"])
    .agg(F.avg("sqft"))
)
