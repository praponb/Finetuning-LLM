from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", F.year("launch_date").alias("launch_year"))
    .agg(F.avg("cost").alias("avg_cost"))
    .orderBy(F.col("avg_cost").desc())
)
