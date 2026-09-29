from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        F.min("quantity").over(w).alias("order_id_min_quantity"),
    )
    .orderBy("order_id")
)
