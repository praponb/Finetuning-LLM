from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "status",
        "carrier",
        "delivered_date",
        "cost",
        F.lag("cost", 2).over(w).alias("prev_cost"),
    )
    .orderBy(F.col("carrier").desc())
)
