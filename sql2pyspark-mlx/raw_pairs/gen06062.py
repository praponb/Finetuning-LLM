from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("price").desc())

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select("id", "brand", "price", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
)
