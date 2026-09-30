from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.round(F.avg("sqft"), 2).alias("avg_sqft"))
    .orderBy("city")
)
