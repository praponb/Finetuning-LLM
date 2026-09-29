from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNull() & (F.col("sqft") == 9149))
    .cube(
        F.quarter("opened_date").alias("opened_quarter"),
        F.when(F.col("sqft") >= 13517, "high").when(F.col("sqft") >= 7544, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.min("sqft").alias("min_sqft"))
    .orderBy(F.col("min_sqft").desc())
)
