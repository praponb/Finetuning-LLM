from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "launch_date"), "brand").orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "launch_date",
        "brand",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_4"),
    )
)
