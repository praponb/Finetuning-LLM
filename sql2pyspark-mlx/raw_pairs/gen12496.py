from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "status",
        F.row_number().over(w).alias("row_seq"),
        F.lag("cost", 1).over(w2).alias("prev_cost"),
    )
)
