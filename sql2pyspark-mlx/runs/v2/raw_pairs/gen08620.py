from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "order_id",
        "unit_price",
        F.row_number().over(w).alias("unit_price_row"),
    )
)
