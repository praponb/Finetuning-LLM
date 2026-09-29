from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
)
