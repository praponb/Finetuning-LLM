from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")
w2 = Window.partitionBy("supplier_id", "brand").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("category") == "Electronics")
    .select(
        "id",
        "category",
        "launch_date",
        "supplier_id",
        "brand",
        "stock_qty",
        F.row_number().over(w).alias("row_num"),
        F.sum("stock_qty").over(w2).alias("running_stock_qty"),
    )
)
