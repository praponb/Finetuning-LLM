from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNotNull())
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.max("sqft").alias("max_sqft"))
)
