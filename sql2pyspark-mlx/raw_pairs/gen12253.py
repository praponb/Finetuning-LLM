from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("quantity")

scored = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.last("quantity", ignorenulls=True).over(w2).alias("last_known_quantity"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
        F.cume_dist().over(w4).alias("quantity_cume_dist"),
    )
)

result = (
    scored
    .groupBy("product_id")
    .agg(
        F.percentile_approx("cumulative_quantity", 0.9).alias("cumulative_quantity_p90"),
        F.avg("quantity_cume_dist").alias("avg_quantity_cume_dist"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
    .filter(F.col("cumulative_quantity_p90") < 1000000)
    .orderBy(F.col("quantity_bit_and").desc())
)
