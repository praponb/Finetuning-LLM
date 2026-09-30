from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())
w2 = Window.orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.rank().over(w).alias("quantity_rank"),
        F.lag("quantity", 2, 0).over(w2).alias("prev_quantity"),
    )
)
