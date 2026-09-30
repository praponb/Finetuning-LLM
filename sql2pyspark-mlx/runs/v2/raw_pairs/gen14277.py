from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("price").desc())

result = (
    df_products
    .filter(F.col("cost") <= 680)
    .select("id", "brand", "price", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "brand", "price")
    .orderBy("brand")
)
