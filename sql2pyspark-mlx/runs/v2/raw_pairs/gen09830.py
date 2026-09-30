from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("supplier_id")

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .groupBy("supplier_id")
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
    .orderBy("total_cost")
)
