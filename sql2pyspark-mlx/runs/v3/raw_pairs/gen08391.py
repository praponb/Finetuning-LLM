from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_order_items
    .filter(F.col("unit_price").between(300, 320))
    .select(
        "id",
        "product_id",
        "unit_price",
        "order_id",
        F.last("order_id").over(w).alias("last_order_id"),
    )
    .orderBy("order_id")
)
