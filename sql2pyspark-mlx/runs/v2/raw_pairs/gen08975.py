from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        "quantity",
        F.last("quantity").over(w).alias("last_quantity"),
    )
)
