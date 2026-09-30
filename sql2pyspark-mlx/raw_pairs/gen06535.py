from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc())

result = (
    df_products
    .filter(F.size("tags") > 1)
    .select("id", "category", "price", F.rank().over(w).alias("rnk"))
)
