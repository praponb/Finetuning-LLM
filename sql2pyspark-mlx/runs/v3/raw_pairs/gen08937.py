from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_7"),
    )
    .orderBy("delivered_date")
)
