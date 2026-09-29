from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc())

result = (
    df_products
    .select("id", "category", "price", F.dense_rank().over(w).alias("price_dense_rank"))
)
