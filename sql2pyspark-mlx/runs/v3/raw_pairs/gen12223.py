from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("brand")

result = (
    df_products
    .filter(F.col("launch_date").between("2024-04-01", "2024-08-01"))
    .groupBy("brand")
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
)
