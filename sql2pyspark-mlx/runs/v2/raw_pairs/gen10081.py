from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
)
