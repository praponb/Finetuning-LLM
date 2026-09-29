from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price")

result = (
    df_products
    .select("id", "category", "price", F.dense_rank().over(w).alias("dense_rnk"))
)
