from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("unit_price", "id")
w3 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w4 = Window.orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_first())

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        F.last("unit_price", ignorenulls=True).over(w).alias("last_known_unit_price"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("seq_no_2"),
        F.percent_rank().over(w5).alias("quantity_percent_rank"),
    )
)
