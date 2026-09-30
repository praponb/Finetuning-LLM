from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty")

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .select(
        "id",
        "category",
        "stock_qty",
        F.dense_rank().over(w).alias("stock_qty_dense_rank"),
    )
)
