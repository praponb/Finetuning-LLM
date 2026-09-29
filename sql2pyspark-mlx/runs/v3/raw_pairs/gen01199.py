from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.row_number().over(w).alias("row_num"),
    )
    .orderBy("order_id")
)
