from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.orderBy("shipped_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "status",
        "cost",
        "shipped_date",
        F.avg("cost").over(w).alias("status_avg_cost"),
        F.round(F.avg("cost").over(w2), 2).alias("cost_moving_avg_4"),
    )
)
