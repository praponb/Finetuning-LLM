from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(790, 1160))
    .groupBy("category")
    .agg(F.min("cost").alias("min_cost"))
    .filter(F.col("min_cost") < 520)
)
