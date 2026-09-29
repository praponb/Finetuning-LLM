from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("product_id")

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-03-01", "2024-04-01"))
    .groupBy("product_id")
    .agg(F.sum("units").alias("total_units"))
    .withColumn("cumulative_units", F.sum("total_units").over(w))
    .orderBy(F.col("cumulative_units").desc(), "product_id")
    .limit(20)
)
