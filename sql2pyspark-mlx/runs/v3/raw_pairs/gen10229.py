from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "launch_date",
        "price",
        F.round(F.avg("price").over(w), 2).alias("price_moving_avg_7"),
    )
)
