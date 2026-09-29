from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select(
        "id",
        "brand",
        "launch_date",
        "stock_qty",
        F.sum("stock_qty").over(w).alias("running_stock_qty"),
    )
)
