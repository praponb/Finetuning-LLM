from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        "order_id",
        F.last("order_id").over(w).alias("last_order_id"),
    )
)
