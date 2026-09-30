from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

result = (
    df_products
    .select(
        "id",
        "category",
        "stock_qty",
        F.sum("stock_qty").over(w).alias("category_sum_stock_qty"),
    )
)
