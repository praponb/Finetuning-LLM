from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "unit_price", "quantity")
    .orderBy("id")
)
