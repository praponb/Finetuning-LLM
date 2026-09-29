from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.lead("unit_price", 2, 0).over(w).alias("next_unit_price"),
    )
    .orderBy("id")
)
