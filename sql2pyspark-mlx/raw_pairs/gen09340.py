from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.first("quantity").over(w2).alias("first_quantity"),
    )
    .orderBy("quantity", F.col("first_quantity").desc())
)
