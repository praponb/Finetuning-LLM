from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "order_id",
        "unit_price",
        "quantity",
        F.first("quantity").over(w).alias("first_quantity"),
        F.last("order_id").over(w2).alias("last_order_id"),
    )
)
