from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 13432, "high").when(F.col("sqft") >= 5796, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.max("sqft").alias("max_sqft"))
    .filter(F.col("max_sqft") < 4500)
)
