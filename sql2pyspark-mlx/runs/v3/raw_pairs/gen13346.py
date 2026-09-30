from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price")
w2 = Window.partitionBy("brand")

result = (
    df_products
    .select(
        "id",
        "category",
        "price",
        "brand",
        F.rank().over(w).alias("price_rank"),
        F.round((F.col("price") / F.sum("price").over(w2)), 3).alias("price_share"),
    )
)
