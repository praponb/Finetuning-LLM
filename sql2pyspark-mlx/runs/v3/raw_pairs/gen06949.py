from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "category")
    .agg(F.sum("cost").alias("total_cost"))
    .filter(F.col("total_cost") > 3200)
    .orderBy("brand")
)
