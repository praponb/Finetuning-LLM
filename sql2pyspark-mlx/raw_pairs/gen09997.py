from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-2, -1)
w5 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_order_items
    .select(
        "id",
        "quantity",
        "unit_price",
        F.sum("unit_price").over(w).alias("running_unit_price"),
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.first("unit_price", ignorenulls=True).over(w3).alias("first_known_unit_price"),
        F.avg("quantity").over(w4).alias("quantity_avg_2_prior"),
        F.sum("quantity").over(w5).alias("cumulative_quantity"),
    )
)

result = (
    scored
    .select(
        "id",
        "quantity",
        "running_unit_price",
        "running_quantity",
        "quantity_avg_2_prior",
        "cumulative_quantity",
    )
    .orderBy(F.col("quantity").asc_nulls_first())
)
