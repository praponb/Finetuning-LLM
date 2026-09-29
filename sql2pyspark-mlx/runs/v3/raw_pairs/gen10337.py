from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("cost")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "delivered_date",
        "cost",
        "order_id",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_4"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
