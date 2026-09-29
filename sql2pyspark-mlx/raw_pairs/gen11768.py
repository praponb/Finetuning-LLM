from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        F.max("unit_price").over(w).alias("order_id_max_unit_price"),
    )
)
