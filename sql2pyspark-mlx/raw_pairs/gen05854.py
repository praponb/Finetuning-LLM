from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "unit_price",
        F.lead("unit_price").over(w).alias("next_unit_price"),
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.row_number().over(w).alias("row_seq"),
    )
)
