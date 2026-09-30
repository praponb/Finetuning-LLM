from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "delivered_date",
        F.last("delivered_date").over(w).alias("last_delivered_date"),
    )
)
