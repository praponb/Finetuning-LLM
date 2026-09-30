from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.sum("unit_price").alias("sum_order_item_unit_price"))

with_windows = (
    df_order_items
    .select(
        "id",
        "product_id",
        F.lag("unit_price").over(w).alias("prev_unit_price"),
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.row_number().over(w).alias("event_seq"),
    )
)

result = (
    with_windows
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "prev_unit_price",
        "prev_quantity",
        "next_quantity",
        "event_seq",
        F.col("sum_order_item_unit_price"),
    )
    .orderBy("next_quantity")
)
