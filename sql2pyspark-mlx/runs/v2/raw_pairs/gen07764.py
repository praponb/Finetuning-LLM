from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(F.countDistinct("age").alias("unique_citys"))
)
