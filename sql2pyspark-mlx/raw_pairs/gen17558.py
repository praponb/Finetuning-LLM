from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w2 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.ntile(4).over(w).alias("unit_price_bucket"),
        F.first("unit_price", ignorenulls=True).over(w2).alias("first_known_unit_price"),
        F.lag("quantity", 1).over(w3).alias("prev_quantity"),
    )
)
