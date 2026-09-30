from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost", "id")
w2 = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "category",
        "cost",
        "brand",
        "launch_date",
        "stock_qty",
        F.row_number().over(w).alias("row_num"),
        F.sum("stock_qty").over(w2).alias("running_stock_qty"),
    )
)
