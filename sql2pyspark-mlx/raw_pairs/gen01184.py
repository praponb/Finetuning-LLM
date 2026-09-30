from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("launch_date"), "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "stock_qty",
        "launch_date",
        F.row_number().over(w).alias("row_seq"),
        F.first("cost", ignorenulls=True).over(w2).alias("first_known_cost"),
        F.sum("stock_qty").over(w3).alias("cumulative_stock_qty"),
        F.row_number().over(w4).alias("seq_no"),
    )
)
