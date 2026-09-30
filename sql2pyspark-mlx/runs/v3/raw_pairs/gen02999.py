from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")
w2 = Window.partitionBy("order_id", "status").orderBy("delivered_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "status",
        "delivered_date",
        "cost",
        "order_id",
        F.sum("cost").over(w).alias("running_cost"),
        F.round(F.avg("cost").over(w2), 2).alias("cost_moving_avg_4"),
    )
)
