from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2024-05-01")
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
    .orderBy("delivered_date")
)
