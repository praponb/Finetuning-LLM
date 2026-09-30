from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty", "id")

result = (
    df_products
    .select(
        "id",
        "brand",
        "stock_qty",
        "price",
        F.first("price").over(w).alias("first_price"),
    )
)
