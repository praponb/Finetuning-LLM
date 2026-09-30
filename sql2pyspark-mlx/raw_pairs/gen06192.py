from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([8, 10, 24, 28]))
    .select(
        "id",
        "product_id",
        "unit_price",
        F.min("unit_price").over(w).alias("product_id_min_unit_price"),
    )
)
