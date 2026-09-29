from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc())

result = (
    df_products
    .select("id", "price", F.dense_rank().over(w).alias("dense_rnk"))
    .orderBy(F.col("price").desc())
)
