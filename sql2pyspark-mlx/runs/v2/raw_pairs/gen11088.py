from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id")
w2 = Window.partitionBy("order_id").orderBy("cost", "id")

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([3, 5, 8]))
    .select(
        "id",
        "status",
        "order_id",
        "cost",
        F.min("cost").over(w).alias("status_min_cost"),
        F.first("status").over(w2).alias("first_status"),
    )
)
