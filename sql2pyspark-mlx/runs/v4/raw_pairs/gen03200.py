from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status")

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2024-03-01")
    .select(
        "id",
        "order_id",
        "status",
        "cost",
        F.sum("cost").over(w).alias("order_id_sum_cost"),
    )
    .orderBy("status")
)
