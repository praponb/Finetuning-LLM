from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("product_name").like("%Water%"))
    .select(
        "id",
        "category",
        "launch_date",
        "stock_qty",
        F.lead("stock_qty", 1).over(w).alias("next_stock_qty"),
    )
    .orderBy("launch_date")
)
