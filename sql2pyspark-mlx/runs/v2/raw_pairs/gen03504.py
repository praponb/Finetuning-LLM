from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.lag("unit_price", 1).over(w).alias("prev_unit_price"),
        (F.col("unit_price") - F.lag("unit_price", 1, 0).over(w2)).alias("unit_price_change"),
    )
)
