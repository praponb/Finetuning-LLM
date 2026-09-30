from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.max("quantity").over(w).alias("product_id_max_quantity"),
        F.round(F.avg("unit_price").over(w2), 2).alias("unit_price_moving_avg_7"),
    )
)
