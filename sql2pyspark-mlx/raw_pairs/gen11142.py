from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        F.max("stock_qty").over(w).alias("last3_max_stock_qty"),
        F.count("stock_qty").over(w).alias("last3_count_stock_qty"),
        F.sum("stock_qty").over(w).alias("last3_sum_stock_qty"),
    )
)
