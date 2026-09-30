from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("quantity") > 4)
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.min("unit_price").over(w).alias("order_id_min_unit_price"),
        F.lead("quantity", 1, 0).over(w2).alias("next_quantity"),
    )
)
