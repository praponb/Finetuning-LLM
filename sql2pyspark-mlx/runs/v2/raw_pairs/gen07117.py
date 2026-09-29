from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "order_id",
        "cost",
        "shipped_date",
        F.avg("cost").over(w).alias("status_avg_cost"),
        F.rank().over(w2).alias("rnk"),
    )
)
