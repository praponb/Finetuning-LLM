from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.col("order_id") == 7)
    .select(
        "id",
        "carrier",
        "order_id",
        "delivered_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_3"),
        F.ntile(10).over(w2).alias("bucket"),
    )
)
