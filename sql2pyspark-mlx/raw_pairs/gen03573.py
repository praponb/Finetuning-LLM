from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc())

result = (
    df_products
    .select("id", "category", "cost", "price", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
)
