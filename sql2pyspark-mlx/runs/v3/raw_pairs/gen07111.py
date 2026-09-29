from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("carrier")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "cost",
        "carrier",
        F.avg("cost").over(w).alias("order_id_avg_cost"),
        F.min("cost").over(w2).alias("carrier_min_cost"),
    )
)
