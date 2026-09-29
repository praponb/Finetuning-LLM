from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost")

result = (
    df_products
    .select("id", "category", "cost", "brand", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 1)
    .select("id", "category", "cost", "brand")
)
