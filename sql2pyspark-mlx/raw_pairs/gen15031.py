from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("order_id", "product_id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([3, 31, 37]))
    .select(
        "id",
        "product_id",
        "unit_price",
        "order_id",
        "quantity",
        F.last("order_id").over(w).alias("last_order_id"),
        F.max("quantity").over(w2).alias("order_id_max_quantity"),
    )
)
