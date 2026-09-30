from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id")

cleaned = (
    df_order_items
    .filter(F.col("quantity") == 5)
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

result = (
    cleaned
    .filter(F.col("order_id").isin([10, 13, 32, 33]))
    .select(
        "id",
        "order_id",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.last("quantity_filled", ignorenulls=True).over(w2).alias("last_known_quantity_filled"),
        F.sum("unit_price").over(w3).alias("product_id_sum_unit_price"),
    )
)
