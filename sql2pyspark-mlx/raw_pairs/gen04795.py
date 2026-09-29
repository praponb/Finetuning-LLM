from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id", "product_id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([23, 29, 32, 34]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.lead("unit_price").over(w_hist).alias("next_unit_price"),
        F.lag("unit_price").over(w_hist).alias("prev_unit_price"),
        F.lead("quantity").over(w_hist).alias("next_quantity"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4).alias("unit_price_share"),
    )
)
