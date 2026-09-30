from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id").orderBy("unit_price", "id")
w_part_2 = Window.partitionBy("order_id")
w_ordered = Window.orderBy("quantity", "id")

with_windows = (
    df_order_items
    .select(
        "*",
        F.lag("quantity", 2, 0).over(w_part).alias("prev_quantity"),
        F.count("unit_price").over(w_part_2).alias("order_id_count_unit_price"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

result = (
    with_windows
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 400, "HIGH").otherwise("LOW"))
    .withColumn("prev_quantity_tier", F.when(F.col("prev_quantity") >= 8, "PREMIUM").otherwise("STANDARD"))
    .rollup("unit_price_tier", "prev_quantity_tier")
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.grouping("unit_price_tier").alias("grouping_id"),
    )
    .orderBy(F.col("unit_price_tier").desc())
)
