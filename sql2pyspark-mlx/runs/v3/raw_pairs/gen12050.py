from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price")

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .select("id", "brand", "price", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
)
