from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "category").orderBy(F.col("stock_qty").desc(), "id")
w2 = Window.partitionBy("category").orderBy("cost", "id")

result = (
    df_products
    .filter(F.col("stock_qty") == 136)
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        F.ntile(5).over(w).alias("stock_qty_bucket"),
        F.ntile(4).over(w2).alias("cost_bucket"),
    )
)
