from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "product_id",
        "quantity",
        F.row_number().over(w).alias("rn"),
        F.sum("unit_price").over(w2).alias("running_unit_price"),
    )
    .orderBy("unit_price", F.col("product_id").desc())
)
