from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("unit_price") <= 180)
    .select(
        "id",
        "product_id",
        "order_id",
        "quantity",
        "unit_price",
        F.lag("unit_price", 1).over(w).alias("prev_unit_price"),
    )
)
