from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "category",
        "launch_date",
        "stock_qty",
        F.first("stock_qty").over(w).alias("first_stock_qty"),
    )
)
