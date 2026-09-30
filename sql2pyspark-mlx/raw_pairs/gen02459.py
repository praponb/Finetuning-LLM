from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "cost",
        "status",
        F.sum("cost").over(w).alias("cumulative_cost"),
        F.sum("cost").over(w2).alias("running_cost"),
    )
)
