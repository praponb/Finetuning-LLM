from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "stock_qty",
        F.lead("stock_qty", 1, 0).over(w).alias("next_stock_qty"),
    )
    .orderBy("launch_date")
)
