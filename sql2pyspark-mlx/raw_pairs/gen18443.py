from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("delivered_date"), "status")
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        "status",
        "cost",
        "order_id",
        F.min("cost").over(w).alias("delivered_date_month_min_cost"),
        F.row_number().over(w2).alias("rn"),
    )
)
