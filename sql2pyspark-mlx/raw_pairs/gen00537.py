from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("launch_date"), "supplier_id").orderBy("launch_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.avg("stock_qty").over(w).alias("stock_qty_avg_2"),
        F.ntile(4).over(w2).alias("stock_qty_bucket"),
    )
)
