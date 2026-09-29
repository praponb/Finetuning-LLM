from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .filter(F.col("quantity") < 5)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.sum("unit_price").over(w).alias("running_unit_price"),
    )
)
