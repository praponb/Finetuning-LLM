from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("greatest_quantity_unit_price", F.greatest("quantity", "unit_price"))
)

result = (
    base
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.min("quantity").over(w_running).alias("product_id_min_quantity"),
        F.sum("unit_price").over(w2).alias("cumulative_unit_price"),
    )
)
