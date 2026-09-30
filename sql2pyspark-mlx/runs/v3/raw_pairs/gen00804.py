from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_3"),
    )
)
