from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "*",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.lag("unit_price").over(w).alias("prev_unit_price"),
        F.row_number().over(w).alias("event_seq"),
    )
)
