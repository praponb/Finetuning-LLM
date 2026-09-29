from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        F.lead("unit_price").over(w_part).alias("next_unit_price"),
        F.lag("quantity").over(w_part).alias("prev_quantity"),
        F.lead("quantity").over(w_part).alias("next_quantity"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)
