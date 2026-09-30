from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

result = (
    df_products
    .filter(F.length("product_name") < 6)
    .select(
        "id",
        "category",
        "stock_qty",
        F.max("stock_qty").over(w).alias("category_max_stock_qty"),
    )
)
