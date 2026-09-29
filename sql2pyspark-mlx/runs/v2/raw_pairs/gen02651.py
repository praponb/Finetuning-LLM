from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_order_items
    .filter(F.col("order_id") == 5)
    .select(
        "id",
        "product_id",
        "unit_price",
        F.max("unit_price").over(w).alias("product_id_max_unit_price"),
    )
)
