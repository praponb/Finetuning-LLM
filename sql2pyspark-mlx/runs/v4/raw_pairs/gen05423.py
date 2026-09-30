from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.max("unit_price").over(w2).alias("product_id_max_unit_price"),
    )
)
