from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .filter(F.col("unit_price") >= 230)
    .select(
        "id",
        "quantity",
        "product_id",
        "unit_price",
        F.rank().over(w).alias("rnk"),
        F.sum("unit_price").over(w2).alias("running_unit_price"),
    )
)
