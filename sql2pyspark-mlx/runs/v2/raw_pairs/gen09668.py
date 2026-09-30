from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-09-01", "2024-07-27"))
    .select(
        "id",
        "status",
        "delivered_date",
        "cost",
        "order_id",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.round(F.avg("cost").over(w2), 2).alias("cost_moving_avg_3"),
    )
)
