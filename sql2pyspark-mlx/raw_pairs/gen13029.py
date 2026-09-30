from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_first())
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        F.first("quantity", ignorenulls=True).over(w).alias("first_known_quantity"),
        F.rank().over(w2).alias("quantity_rank"),
        F.sum("unit_price").over(w3).alias("order_id_sum_unit_price"),
        F.sum("unit_price").over(w4).alias("cumulative_unit_price"),
    )
)
