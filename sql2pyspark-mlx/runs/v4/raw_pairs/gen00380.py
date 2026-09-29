from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"), "region")
    .agg(F.avg("sqft").alias("avg_sqft"))
    .orderBy(F.col("avg_sqft").desc())
)
