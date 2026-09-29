from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "product_id",
        F.last("product_id").over(w).alias("last_product_id"),
    )
)
