from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "launch_date"), "brand").orderBy("launch_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "launch_date",
        "brand",
        "price",
        F.round(F.avg("price").over(w), 2).alias("price_moving_avg_3"),
    )
)
