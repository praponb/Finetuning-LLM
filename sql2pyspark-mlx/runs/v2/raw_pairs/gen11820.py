from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.min("cost").alias("min_cost"))
)
