from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(-3, -1)
w2 = Window.orderBy("cost", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.avg("price").over(w).alias("price_avg_3_prior"),
        F.avg("stock_qty").over(w2).alias("stock_qty_avg_2"),
    )
)
