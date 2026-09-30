from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .filter(F.col("product_id").isin([12, 27, 34]))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.lead("unit_price").over(w_running).alias("next_unit_price"),
        F.lead("quantity").over(w_running).alias("next_quantity"),
        F.lag("quantity").over(w_running).alias("prev_quantity"),
        F.row_number().over(w_running).alias("row_seq"),
        F.last("quantity", ignorenulls=True).over(w2).alias("last_known_quantity"),
    )
)
