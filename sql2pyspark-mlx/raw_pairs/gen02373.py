from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("launch_date"), "brand").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("price") < 1010)
    .select(
        "id",
        "launch_date",
        "brand",
        "stock_qty",
        F.lag("stock_qty", 2, 0).over(w).alias("prev_stock_qty"),
    )
)
