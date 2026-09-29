from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.lag("quantity", 2, 0).over(w).alias("prev_quantity"),
    )
)
