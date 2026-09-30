from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .select(
        "id",
        "brand",
        "launch_date",
        "stock_qty",
        F.sum("stock_qty").over(w).alias("running_stock_qty"),
    )
)
