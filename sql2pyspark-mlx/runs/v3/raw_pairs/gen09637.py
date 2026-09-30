from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc())

result = (
    df_products
    .filter(F.col("launch_date") < "2023-05-01")
    .select("id", "category", "price", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
)
