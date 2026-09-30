from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("quantity") != 3)
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") - F.lag("quantity", 1).over(w)).alias("quantity_change"),
        (F.col("quantity") - F.lag("quantity", 2, 0).over(w2)).alias("quantity_change_2"),
    )
    .orderBy(F.col("quantity").desc())
)
