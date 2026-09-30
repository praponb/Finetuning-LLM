from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id")
w3 = Window.orderBy(F.col("unit_price_scaled").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("order_id").isin([4, 6, 20, 36]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.round((F.col("unit_price") * 1.1), 2).alias("unit_price_scaled"),
    )
)

result = (
    filtered
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price_scaled",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.ntile(4).over(w2).alias("quantity_bucket"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
