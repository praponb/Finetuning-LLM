from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        F.lag("cost", 2, 0).over(w).alias("prev_cost"),
    )
)
