from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        "order_id",
        "unit_price",
        F.first("order_id").over(w).alias("first_order_id"),
        F.sum("unit_price").over(w2).alias("running_unit_price"),
    )
)
