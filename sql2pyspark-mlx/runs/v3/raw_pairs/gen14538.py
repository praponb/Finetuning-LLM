from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .filter(F.col("quantity") == 6)
    .select(
        "id",
        "order_id",
        "unit_price",
        "product_id",
        "quantity",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.sum("unit_price").over(w2).alias("running_unit_price"),
    )
)
