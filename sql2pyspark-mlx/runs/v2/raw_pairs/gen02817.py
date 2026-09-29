from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost", "id")
w2 = Window.partitionBy("category", "brand").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .filter(F.col("launch_date").between("2024-01-01", "2025-06-01"))
    .select(
        "id",
        "category",
        "cost",
        "stock_qty",
        "brand",
        F.first("stock_qty").over(w).alias("first_stock_qty"),
        F.row_number().over(w2).alias("row_num"),
    )
)
