from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "launch_date",
        "stock_qty",
        F.lag("stock_qty", 1).over(w).alias("prev_stock_qty"),
    )
    .orderBy("stock_qty", "launch_date")
)
