from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("product_id") == 26)
    .select(
        "id",
        "product_id",
        "quantity",
        F.lead("quantity", 2, 0).over(w).alias("next_quantity"),
    )
    .orderBy("next_quantity", "id")
)
