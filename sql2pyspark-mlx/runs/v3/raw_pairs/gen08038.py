from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy("launch_date", "id")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("launch_date") > "2024-06-11")
    .select(
        "id",
        "supplier_id",
        "category",
        "launch_date",
        "stock_qty",
        "cost",
        F.lag("stock_qty", 1).over(w).alias("prev_stock_qty"),
        F.lag("cost", 2).over(w2).alias("prev_cost"),
    )
    .orderBy("prev_stock_qty", F.col("launch_date").desc())
)
