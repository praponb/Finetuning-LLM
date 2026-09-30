from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category", "brand")
w3 = Window.partitionBy("category")
w4 = Window.partitionBy("brand").orderBy("cost", "id")

result = (
    df_products
    .select(
        "*",
        F.sum("cost").over(w).alias("cumulative_cost"),
        F.avg("price").over(w2).alias("category_avg_price"),
        F.sum("stock_qty").over(w3).alias("category_sum_stock_qty"),
        F.lag("price", 2).over(w4).alias("prev_price"),
    )
)
