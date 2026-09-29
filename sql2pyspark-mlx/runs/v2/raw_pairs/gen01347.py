from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        F.lag("cost", 1, 0).over(w).alias("prev_cost"),
    )
)
