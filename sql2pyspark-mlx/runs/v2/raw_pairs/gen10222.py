from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc())

result = (
    df_products
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "brand", "stock_qty", "product_name")
)
