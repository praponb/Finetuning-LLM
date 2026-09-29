from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("product_name").like("%Smar%"))
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "stock_qty",
        F.lag("stock_qty", 2).over(w).alias("prev_stock_qty"),
    )
    .orderBy("prev_stock_qty")
)
