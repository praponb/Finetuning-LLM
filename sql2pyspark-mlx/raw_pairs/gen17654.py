from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("cost").between(44, 53))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "order_id",
        F.first("order_id").over(w).alias("first_order_id"),
    )
)
