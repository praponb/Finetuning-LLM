from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "launch_date",
        "stock_qty",
        "category",
        "brand",
        F.sum("stock_qty").over(w).alias("running_stock_qty"),
        F.first("brand").over(w2).alias("first_brand"),
    )
    .orderBy("running_stock_qty")
)
