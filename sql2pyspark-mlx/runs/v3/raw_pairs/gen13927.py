from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy(F.col("launch_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .filter(F.col("cost") <= 510)
    .select(
        "id",
        "supplier_id",
        "category",
        "launch_date",
        "product_name",
        "stock_qty",
        F.last("product_name").over(w).alias("last_product_name"),
        F.sum("stock_qty").over(w2).alias("running_stock_qty"),
    )
    .orderBy(F.col("launch_date").desc(), F.col("running_stock_qty").desc())
)
