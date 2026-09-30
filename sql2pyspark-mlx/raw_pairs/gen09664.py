from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id", "order_id")

result = (
    df_order_items
    .filter(F.col("order_id") == 20)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.lag("unit_price", 1).over(w).alias("prev_unit_price"),
        F.corr("quantity", "unit_price").over(w2).alias("corr_quantity_unit_price"),
    )
)
