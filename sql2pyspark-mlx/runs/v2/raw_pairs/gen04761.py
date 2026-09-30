from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("cost") < 77)
    .select(
        "id",
        "status",
        "delivered_date",
        "cost",
        F.lag("cost", 1, 0).over(w).alias("prev_cost"),
    )
)
