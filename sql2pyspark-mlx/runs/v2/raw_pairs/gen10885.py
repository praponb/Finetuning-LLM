from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("order_id", "product_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        "product_id",
        F.last("quantity").over(w).alias("last_quantity"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 3).alias("unit_price_share"),
    )
    .orderBy("unit_price")
)
