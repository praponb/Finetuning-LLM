from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("order_id").isin([19, 22, 31]))
    .select(
        "id",
        "carrier",
        "cost",
        "delivered_date",
        F.row_number().over(w).alias("rn"),
        F.round(F.avg("cost").over(w2), 2).alias("cost_moving_avg_4"),
    )
)
