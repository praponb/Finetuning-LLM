from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_units").desc())

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.sum("units").alias("total_units"))
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("product_id").desc())
)
