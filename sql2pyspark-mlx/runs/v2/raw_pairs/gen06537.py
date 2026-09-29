from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost")
w2 = Window.partitionBy("brand")

result = (
    df_products
    .select(
        "id",
        "category",
        "cost",
        "brand",
        "stock_qty",
        F.percent_rank().over(w).alias("cost_pct_rank"),
        F.min("stock_qty").over(w2).alias("brand_min_stock_qty"),
    )
    .orderBy("brand")
)
