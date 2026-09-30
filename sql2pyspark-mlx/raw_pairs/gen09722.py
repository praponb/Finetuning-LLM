from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")
w2 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.lag("unit_price").over(w).alias("prev_unit_price"),
        F.row_number().over(w).alias("event_seq"),
        F.last("unit_price", ignorenulls=True).over(w2).alias("last_known_unit_price"),
    )
)
