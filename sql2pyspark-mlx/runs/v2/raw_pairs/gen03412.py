from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("quantity") == 6)
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.sum("unit_price").over(w).alias("running_unit_price"),
    )
    .orderBy("quantity")
)
