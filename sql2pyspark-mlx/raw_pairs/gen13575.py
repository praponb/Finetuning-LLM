from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count("sqft").alias("sqft_count"),
    )
    .filter(F.col("sqft_count") >= 2)
    .drop("sqft_count")
    .orderBy(F.col("opened_year").desc())
)
