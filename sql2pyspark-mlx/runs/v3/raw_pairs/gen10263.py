from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.partitionBy("category").orderBy("price")

result = (
    df_products
    .select(
        "id",
        "category",
        "price",
        F.avg("price").over(w).alias("category_avg_price"),
        F.rank().over(w2).alias("price_rank"),
    )
    .orderBy(F.col("category").desc())
)
