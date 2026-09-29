from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy("sale_date", "id")
w_recent = Window.partitionBy("product_id").orderBy("units", "id")
w3 = Window.partitionBy(F.year("sale_date"), "product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy(F.date_trunc("week", "sale_date"), "store_id").orderBy("units", "id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 4]))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.lag("units", 1).over(w_running).alias("prev_units"),
        F.ntile(10).over(w_recent).alias("units_bucket"),
        F.sum("units").over(w3).alias("cumulative_units"),
        F.lag("revenue", 1).over(w_hist).alias("prev_revenue"),
    )
)
