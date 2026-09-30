from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost")
w2 = Window.partitionBy("category")

result = (
    df_products
    .select(
        "id",
        "brand",
        "cost",
        "category",
        "price",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.round((F.col("price") / F.sum("price").over(w2)), 3).alias("price_share"),
    )
)
