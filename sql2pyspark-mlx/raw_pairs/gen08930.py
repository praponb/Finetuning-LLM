from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "unit_price",
        "quantity",
        F.lag("quantity", 2).over(w).alias("prev_quantity"),
    )
    .orderBy(F.col("quantity").desc())
)
