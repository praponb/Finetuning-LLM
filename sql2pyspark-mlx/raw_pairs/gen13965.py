from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id")

with_windows = (
    df_order_items
    .filter(F.col("product_id") == 34)
    .select(
        "*",
        F.lag("unit_price").over(w_hist).alias("prev_unit_price"),
        F.lead("unit_price").over(w_hist).alias("next_unit_price"),
        F.lead("quantity").over(w_hist).alias("next_quantity"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.min("quantity").over(w2).alias("product_id_min_quantity"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "product_id",
        "prev_unit_price",
        "next_unit_price",
        "next_quantity",
        "product_id_min_quantity",
    )
    .orderBy(F.col("product_id").desc())
)
