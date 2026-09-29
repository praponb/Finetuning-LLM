from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id")

result = (
    df_order_items
    .filter(F.col("quantity") != 4)
    .select(
        "id",
        "product_id",
        "order_id",
        "unit_price",
        F.sum("unit_price").over(w).alias("product_id_sum_unit_price"),
    )
)
