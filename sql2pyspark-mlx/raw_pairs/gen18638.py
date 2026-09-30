from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.when(F.col("sqft") >= 15674, "high").when(F.col("sqft") >= 5769, "mid").otherwise("low").alias("sqft_bucket"))
    .pivot("city", ["Berlin", "New York"])
    .agg(F.count("sqft"))
)
