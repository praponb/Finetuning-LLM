from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "cost",
        F.avg("cost").over(w).alias("status_avg_cost"),
        F.sum("cost").over(w).alias("status_sum_cost"),
    )
)
