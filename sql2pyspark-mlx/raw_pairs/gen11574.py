from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost")
w2 = Window.partitionBy("category").orderBy("stock_qty")

result = (
    df_products
    .select(
        "id",
        "brand",
        "cost",
        "category",
        "stock_qty",
        F.dense_rank().over(w).alias("cost_dense_rank"),
        F.rank().over(w2).alias("stock_qty_rank"),
    )
    .orderBy("stock_qty")
)
