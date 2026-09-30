from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
    .orderBy("running_cost")
)
