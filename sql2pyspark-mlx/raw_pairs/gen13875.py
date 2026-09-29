from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id")

with_windows = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.lead("unit_price").over(w).alias("next_unit_price"),
        F.lag("unit_price").over(w).alias("prev_unit_price"),
        F.row_number().over(w).alias("seq_no"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4).alias("unit_price_share"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "next_quantity",
        "next_unit_price",
        "unit_price_share",
    )
    .orderBy("order_id")
)
