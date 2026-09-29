from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.lead("quantity", 1, 0).over(w).alias("next_quantity"),
        F.lag("quantity", 1).over(w2).alias("prev_quantity"),
    )
)
