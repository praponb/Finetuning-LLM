from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "order_id",
        "quantity",
        "unit_price",
        F.last("unit_price").over(w).alias("last_unit_price"),
    )
)
