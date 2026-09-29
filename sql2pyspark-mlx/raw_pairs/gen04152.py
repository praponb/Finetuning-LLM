from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        F.min("unit_price").over(w).alias("last3_min_unit_price"),
        F.max("unit_price").over(w).alias("last3_max_unit_price"),
        F.avg("unit_price").over(w).alias("last3_avg_unit_price"),
        F.sum("quantity").over(w2).alias("cumulative_quantity"),
    )
)
