from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "product_id",
        "quantity",
        F.sum("unit_price").over(w).alias("running_unit_price"),
        F.round((F.col("quantity") / F.sum("quantity").over(w2)), 3).alias("quantity_share"),
    )
    .orderBy(F.col("unit_price").desc())
)
