from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.sum("quantity").over(w).alias("running_quantity"),
    )
    .orderBy("quantity", "running_quantity")
)
