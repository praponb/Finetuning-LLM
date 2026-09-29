from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "launch_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_3"),
    )
)
