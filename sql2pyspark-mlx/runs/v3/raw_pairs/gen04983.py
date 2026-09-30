from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([3, 9, 10]))
    .select(
        "id",
        "status",
        "cost",
        "carrier",
        "delivered_date",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
        F.round(F.avg("cost").over(w2), 2).alias("cost_moving_avg_3"),
    )
)
