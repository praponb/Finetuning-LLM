from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty", "id")
w2 = Window.orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "brand",
        "stock_qty",
        "price",
        F.row_number().over(w).alias("rn"),
        F.ntile(4).over(w2).alias("quartile"),
    )
)
