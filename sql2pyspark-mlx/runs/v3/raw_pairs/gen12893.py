from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("unit_price") <= 390)
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "order_id",
        (F.col("unit_price") - F.lag("unit_price", 1).over(w)).alias("unit_price_change"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
)
