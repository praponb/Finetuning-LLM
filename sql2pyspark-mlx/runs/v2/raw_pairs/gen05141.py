from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.sum("cost").alias("total_cost"))
    .orderBy(F.col("total_cost").desc(), "category")
)
