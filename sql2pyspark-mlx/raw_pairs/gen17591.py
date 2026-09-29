from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 17329, "high").when(F.col("sqft") >= 2946, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.count("*").alias("total_count"),
    )
)
