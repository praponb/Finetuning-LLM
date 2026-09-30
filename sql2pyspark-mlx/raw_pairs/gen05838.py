from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.avg("cost").alias("avg_cost"))
)
