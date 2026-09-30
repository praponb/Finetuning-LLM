from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy("launch_date", "id")
w2 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")

result = (
    df_products
    .filter(F.col("category").isin(["Home", "Grocery", "Books"]))
    .select(
        "id",
        "brand",
        "supplier_id",
        "launch_date",
        "stock_qty",
        F.sum("stock_qty").over(w).alias("running_stock_qty"),
        F.row_number().over(w2).alias("row_num"),
    )
    .orderBy("launch_date", "running_stock_qty")
)
