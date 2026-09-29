from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([2, 5]))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.max("quantity").over(w).alias("order_id_max_quantity"),
        (F.col("quantity") - F.lag("quantity", 1).over(w2)).alias("quantity_change"),
    )
    .orderBy(F.col("id").desc())
)
