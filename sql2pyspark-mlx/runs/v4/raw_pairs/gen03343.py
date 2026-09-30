from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") == 5238) & (F.col("sqft") < 8658))
    .groupBy(
        F.when(F.col("sqft") >= 14826, "high").when(F.col("sqft") >= 3643, "mid").otherwise("low").alias("sqft_bucket"),
        F.quarter("opened_date").alias("opened_quarter"),
    )
    .agg(F.avg("sqft").alias("avg_sqft"))
    .orderBy(F.col("avg_sqft").desc())
)
