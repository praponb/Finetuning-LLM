from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"), "region")
    .agg(F.sum("sqft").alias("total_sqft"))
    .filter(F.col("total_sqft") < 63000)
    .select("opened_month", "region", "total_sqft")
)
