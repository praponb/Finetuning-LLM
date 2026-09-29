from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "launch_date",
        "stock_qty",
        F.sum("stock_qty").over(w).alias("running_stock_qty"),
    )
    .orderBy(F.col("id").desc())
)
