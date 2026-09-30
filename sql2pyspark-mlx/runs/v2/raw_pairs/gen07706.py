from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id")
w2 = Window.partitionBy("order_id").orderBy("delivered_date")

result = (
    df_shipments
    .select(
        "id",
        "cost",
        "order_id",
        "delivered_date",
        F.row_number().over(w).alias("cost_row"),
        F.rank().over(w2).alias("rnk"),
    )
)
