from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(F.countDistinct("age").alias("unique_citys"))
    .orderBy(F.col("unique_citys").desc(), "city")
)
