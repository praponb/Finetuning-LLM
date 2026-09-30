from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.length("store_name") >= 10) & F.col("sqft").isNotNull()) | F.col("region").isNotNull())
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.round(F.avg("sqft"), 2).alias("avg_sqft"))
)
