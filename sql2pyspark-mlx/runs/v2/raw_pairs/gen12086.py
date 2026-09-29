from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("launch_date").desc(), "id")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("brand") == "Acme")
    .select(
        "id",
        "brand",
        "launch_date",
        "supplier_id",
        "stock_qty",
        F.ntile(2).over(w).alias("launch_date_ntile"),
        F.lag("stock_qty", 1).over(w2).alias("prev_stock_qty"),
    )
    .orderBy("brand")
)
