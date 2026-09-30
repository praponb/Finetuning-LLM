from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .filter(F.col("quantity") <= 4)
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 2)
    .select("id", "order_id", "unit_price", "quantity")
)
