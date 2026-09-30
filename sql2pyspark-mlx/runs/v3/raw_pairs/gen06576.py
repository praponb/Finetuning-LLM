from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([3, 6, 10]))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        F.last("delivered_date").over(w).alias("last_delivered_date"),
    )
)
