from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("week", "launch_date"), "brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "category",
        "cost",
        F.count("stock_qty").over(w).alias("running_count_stock_qty"),
        F.sum("stock_qty").over(w).alias("running_sum_stock_qty"),
        F.max("stock_qty").over(w).alias("running_max_stock_qty"),
        F.last("cost", ignorenulls=True).over(w2).alias("last_known_cost"),
    )
)
