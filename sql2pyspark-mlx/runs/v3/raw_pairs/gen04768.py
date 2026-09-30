from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("order_id") == 35)
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "carrier",
        "shipped_date",
        F.last("cost").over(w).alias("last_cost"),
        F.lag("cost", 1).over(w2).alias("prev_cost"),
    )
)
