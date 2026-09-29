from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy(F.col("max_sqft").desc())
)
