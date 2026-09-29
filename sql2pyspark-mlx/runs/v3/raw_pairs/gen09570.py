from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_3"),
        F.sum("cost").over(w2).alias("running_cost"),
    )
    .orderBy(F.col("delivered_date").desc())
)
