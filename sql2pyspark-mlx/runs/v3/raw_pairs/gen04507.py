from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")
w2 = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_products
    .filter(F.col("supplier_id") == 14)
    .select(
        "id",
        "brand",
        "launch_date",
        "cost",
        "stock_qty",
        F.lead("cost", 2).over(w).alias("next_cost"),
        F.round(F.avg("stock_qty").over(w2), 2).alias("stock_qty_moving_avg_7"),
    )
    .orderBy(F.col("brand").desc())
)
