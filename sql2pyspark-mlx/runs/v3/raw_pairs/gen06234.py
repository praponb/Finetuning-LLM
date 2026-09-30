from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

result = (
    df_products
    .select(
        "id",
        "category",
        "price",
        F.round((F.col("price") / F.sum("price").over(w)), 3).alias("price_share"),
    )
    .orderBy("category")
)
