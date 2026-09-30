from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        "order_id",
        "unit_price",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
        F.lag("unit_price", 1).over(w2).alias("prev_unit_price"),
    )
)
