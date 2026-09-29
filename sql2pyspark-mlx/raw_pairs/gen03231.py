from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc())

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        "cost",
        "order_id",
        "shipped_date",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_3"),
        F.percent_rank().over(w2).alias("shipped_date_pct_rank"),
    )
)
