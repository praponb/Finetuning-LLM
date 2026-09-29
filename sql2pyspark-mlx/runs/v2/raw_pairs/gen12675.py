from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "Flash", "DHL"]))
    .select(
        "id",
        "status",
        "carrier",
        "cost",
        "delivered_date",
        F.ntile(10).over(w).alias("quartile"),
        F.round(F.avg("cost").over(w2), 2).alias("cost_moving_avg_3"),
    )
)
