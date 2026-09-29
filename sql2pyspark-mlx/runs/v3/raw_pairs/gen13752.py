from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 16542, "high").when(F.col("sqft") >= 4658, "mid").otherwise("low").alias("sqft_bucket"),
        "city",
    )
    .agg(F.min("opened_date").alias("min_opened_date"))
    .orderBy("city", "sqft_bucket")
    .limit(3)
)
