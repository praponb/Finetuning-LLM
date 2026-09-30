from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("unit_price") > 260)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") - F.lag("unit_price", 1).over(w)).alias("unit_price_change"),
    )
    .orderBy("quantity")
)
