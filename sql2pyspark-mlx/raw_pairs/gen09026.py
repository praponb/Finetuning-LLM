from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w2 = Window.partitionBy("status")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "delivered_date",
        "status",
        "cost",
        F.row_number().over(w).alias("delivered_date_row"),
        F.avg("cost").over(w2).alias("status_avg_cost"),
    )
    .orderBy(F.col("cost").desc(), "delivered_date_row")
)
