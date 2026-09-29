from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.lag("unit_price", 2).over(w).alias("prev_unit_price"),
        (F.col("quantity") - F.lag("quantity", 1).over(w2)).alias("quantity_change"),
    )
    .orderBy(F.col("id").desc())
)
