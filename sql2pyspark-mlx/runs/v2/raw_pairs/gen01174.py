from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id")
w2 = Window.partitionBy("carrier", "status").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        "carrier",
        "status",
        "shipped_date",
        "cost",
        F.row_number().over(w).alias("row_num"),
        F.lag("cost", 1).over(w2).alias("prev_cost"),
    )
)
