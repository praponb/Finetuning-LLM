from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([3, 13, 20, 22]))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.lead("unit_price").over(w).alias("next_unit_price"),
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.row_number().over(w).alias("row_seq"),
    )
)
