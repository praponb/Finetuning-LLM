from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("cost", "id")
w2 = Window.partitionBy(F.date_trunc("week", "shipped_date"), "status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "status",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.sum("cost").over(w2).alias("running_cost"),
    )
)
