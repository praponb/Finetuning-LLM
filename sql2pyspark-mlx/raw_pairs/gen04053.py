from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("launch_date"), "category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy("stock_qty", "id")
w4 = Window.partitionBy("brand").orderBy("cost", "id")

result = (
    df_products
    .select(
        "id",
        "product_name",
        "cost",
        "stock_qty",
        "launch_date",
        F.sum("stock_qty").over(w).alias("cumulative_stock_qty"),
        F.sum("cost").over(w_recent).alias("running_cost"),
        F.lag("price", 1).over(w3).alias("prev_price"),
        F.row_number().over(w4).alias("row_seq"),
    )
)
