from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "shipped_date",
        "carrier",
        "cost",
        F.first("carrier").over(w).alias("first_carrier"),
        F.lag("cost", 1).over(w).alias("prev_cost"),
    )
)
