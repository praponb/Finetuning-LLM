from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_products
    .filter(F.col("stock_qty") == 152)
    .select(
        "id",
        "category",
        "launch_date",
        "price",
        F.round(F.avg("price").over(w), 2).alias("price_moving_avg_7"),
    )
)
