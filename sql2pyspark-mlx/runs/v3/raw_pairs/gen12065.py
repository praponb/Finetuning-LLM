from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy("shipped_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "order_id",
        "shipped_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_4"),
    )
    .orderBy(F.col("shipped_date").desc())
)
