from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .filter(F.col("unit_price") > 210)
    .select(
        "id",
        "product_id",
        "unit_price",
        F.sum("unit_price").over(w).alias("running_unit_price"),
    )
    .orderBy("id")
)
