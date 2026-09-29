from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.year("shipped_date") == 2023)
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
        (F.col("cost") - F.lag("cost", 1).over(w2)).alias("cost_change"),
    )
    .orderBy("order_id")
)
