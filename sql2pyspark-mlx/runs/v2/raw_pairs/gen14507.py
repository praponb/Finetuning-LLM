from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.sum("unit_price").over(w).alias("running_unit_price"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy(F.col("running_unit_price").desc(), "id")
)
