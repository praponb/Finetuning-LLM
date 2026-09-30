from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id")
w3 = Window.partitionBy("order_id").orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.round((F.col("unit_price") / F.sum("unit_price").over(w)), 4).alias("unit_price_share"),
        F.lag("quantity", 1).over(w2).alias("prev_quantity"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
