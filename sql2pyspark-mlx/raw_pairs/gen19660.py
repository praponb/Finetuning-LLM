from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("launch_date").between("2024-01-01", "2024-04-01"))
    .select(
        "id",
        "brand",
        "launch_date",
        "stock_qty",
        F.lead("stock_qty", 1).over(w).alias("next_stock_qty"),
    )
    .orderBy(F.col("next_stock_qty").desc())
)
