from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .select("id", "delivered_date", "cost", F.sum("cost").over(w).alias("running_cost"))
    .orderBy("id")
)
