from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["London", "Singapore"]))
    .groupBy(F.when(F.col("sqft") >= 15168, "high").when(F.col("sqft") >= 6419, "mid").otherwise("low").alias("sqft_bucket"))
    .pivot("city", ["Singapore", "London", "Bangkok", "New York"])
    .agg(F.count("sqft"))
    .orderBy("sqft_bucket")
)
