from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .filter(F.col("quantity") >= 4)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.min("unit_price").over(w_ordered).alias("last4_min_unit_price"),
        F.avg("unit_price").over(w_ordered).alias("last4_avg_unit_price"),
        F.max("unit_price").over(w_ordered).alias("last4_max_unit_price"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
