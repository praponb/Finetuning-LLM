from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "order_id",
        "quantity",
        F.lead("quantity", 1, 0).over(w).alias("next_quantity"),
    )
)
