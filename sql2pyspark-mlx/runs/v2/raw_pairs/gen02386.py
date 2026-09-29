from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.sum("sqft").alias("total_sqft"))
    .orderBy(F.col("total_sqft").desc())
)
