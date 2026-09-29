from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

result = (
    df_products
    .withColumn("min_price_by_category", F.min("price").over(w))
    .filter(F.col("price") == F.col("min_price_by_category"))
    .select("id", "category", "price", "brand")
)
