from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_products
    .filter(F.col("cost") <= 220)
    .select(
        "id",
        "launch_date",
        "price",
        "stock_qty",
        F.round(F.avg("price").over(w), 2).alias("price_moving_avg_4"),
        F.round(F.avg("stock_qty").over(w), 2).alias("stock_qty_moving_avg_4"),
    )
    .orderBy(F.col("launch_date").desc())
)
