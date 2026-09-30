from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price")

result = (
    df_order_items
    .filter(F.col("order_id") == 6)
    .select(
        "id",
        "product_id",
        "quantity",
        "order_id",
        "unit_price",
        F.row_number().over(w).alias("rn"),
        F.rank().over(w2).alias("unit_price_rank"),
    )
    .orderBy("id")
)
