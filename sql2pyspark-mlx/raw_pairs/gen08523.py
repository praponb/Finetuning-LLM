from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("category", "supplier_id")

result = (
    df_products
    .select(
        "id",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.sum("stock_qty").over(w).alias("cumulative_stock_qty"),
        F.corr("price", "cost").over(w_hist).alias("corr_price_cost"),
    )
)
