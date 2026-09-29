from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_order_items
    .filter(F.col("product_id") == 7)
    .select(
        "id",
        "order_id",
        "unit_price",
        "product_id",
        "quantity",
        F.row_number().over(w).alias("rn"),
        F.round(F.avg("quantity").over(w2), 2).alias("quantity_moving_avg_7"),
    )
    .orderBy(F.col("order_id").desc())
)
