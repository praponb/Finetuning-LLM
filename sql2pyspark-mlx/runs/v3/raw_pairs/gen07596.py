from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc())

result = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .select("id", "category", "price", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "category", "price")
    .orderBy(F.col("id").desc())
)
