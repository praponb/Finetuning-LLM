from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id").orderBy("quantity", "id")
w_running = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)

result = (
    base
    .select(
        "id",
        "order_id",
        F.lag("line_total").over(w_grp).alias("prev_line_total"),
        F.lead("line_total").over(w_grp).alias("next_line_total"),
        F.lead("quantity").over(w_grp).alias("next_quantity"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.sum("line_total").over(w_running).alias("cumulative_line_total"),
    )
)
