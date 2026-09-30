from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "shipped_date",
        "carrier",
        F.last("carrier").over(w).alias("last_carrier"),
    )
)
