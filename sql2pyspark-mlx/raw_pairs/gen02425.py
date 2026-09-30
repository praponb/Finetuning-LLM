from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.count("*").alias("row_count"),
    )
)
