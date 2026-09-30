from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w_running = Window.orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        F.lag("quantity", 2).over(w).alias("prev_quantity"),
        F.lead("quantity", 1).over(w2).alias("next_quantity"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)
