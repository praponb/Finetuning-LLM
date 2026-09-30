from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.avg("unit_price").over(w).alias("running_avg_unit_price"),
        F.sum("unit_price").over(w).alias("running_sum_unit_price"),
        F.max("unit_price").over(w).alias("running_max_unit_price"),
        F.corr("quantity", "unit_price").over(w2).alias("corr_quantity_unit_price"),
    )
)
