from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "product_id",
        "unit_price",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.lead("unit_price", 1).over(w2).alias("next_unit_price"),
    )
)
