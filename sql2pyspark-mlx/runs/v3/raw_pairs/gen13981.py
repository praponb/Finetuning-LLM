from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "category").orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "brand",
        "category",
        "launch_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_4"),
    )
)
