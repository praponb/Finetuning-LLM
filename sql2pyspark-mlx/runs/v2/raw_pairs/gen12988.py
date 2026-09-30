from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("price")

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .select("id", "price", F.dense_rank().over(w).alias("price_dense_rank"))
)
