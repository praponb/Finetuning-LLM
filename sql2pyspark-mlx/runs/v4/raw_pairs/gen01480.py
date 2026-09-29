from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_last())
w3 = Window.orderBy("quantity", "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_last())
w5 = Window.partitionBy("order_id").orderBy("unit_price", "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_str", F.col("quantity").cast("string"))
)

with_dims = (
    base
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.amount"))
)

with_windows = (
    with_dims
    .withColumn("running_amount", F.sum("amount").over(w))
    .withColumn("quantity_cume_dist", F.cume_dist().over(w2))
    .withColumn("next_unit_price", F.lead("unit_price", 2).over(w3))
    .withColumn("unit_price_cume_dist", F.cume_dist().over(w4))
    .withColumn("prev_quantity", F.lag("quantity", 2, 0).over(w5))
)

totals = (
    with_windows
    .groupBy("order_id", "product_id")
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.bit_or("quantity").alias("quantity_bit_or_2"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
    )
)

result = (
    totals
    .select("order_id", "product_id", "quantity_bit_or_2", "num_rows", "amount_p95")
    .orderBy(F.col("amount_p95").asc_nulls_last(), F.col("product_id").desc())
)
