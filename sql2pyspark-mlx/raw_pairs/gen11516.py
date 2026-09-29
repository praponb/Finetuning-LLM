from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")

result = (
    df_products
    .filter(F.col("price") >= 920)
    .select(
        "id",
        "brand",
        "stock_qty",
        F.min("stock_qty").over(w).alias("brand_min_stock_qty"),
    )
)
