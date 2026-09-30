from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.lag("cost").over(w).alias("prev_cost"),
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
