from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price_int", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity_filled").desc())
w3 = Window.partitionBy("order_id").orderBy("quantity_filled", "id")
w4 = Window.partitionBy("order_id")
w_running = Window.partitionBy("order_id").orderBy("unit_price_plus_unit_price_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy("order_id_sum_unit_price_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("last_known_quantity_filled_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("order_id").orderBy("order_id_min_prev_unit_price_int", "id")
w11 = Window.partitionBy("order_id").orderBy(F.col("unit_price_plus_unit_price_int").desc_nulls_last())

valid = (
    df_order_items
    .filter(F.col("unit_price") <= 280)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

base = (
    valid
    .filter(F.col("quantity_filled") < 56)
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity_filled",
        "unit_price_int",
        (F.col("unit_price") + F.col("unit_price_int")).alias("unit_price_plus_unit_price_int"),
    )
)

cleaned = (
    base
    .filter(F.col("quantity_filled") >= 26)
    .withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
)

enriched = (
    cleaned
    .select(
        "*",
        F.ntile(5).over(w).alias("unit_price_int_bucket"),
        F.lag("unit_price_int", 2, 0).over(w).alias("prev_unit_price_int"),
        F.cume_dist().over(w2).alias("quantity_filled_cume_dist"),
        F.ntile(10).over(w3).alias("quantity_filled_bucket"),
        F.sum("unit_price_int").over(w4).alias("order_id_sum_unit_price_int"),
    )
)

enriched_order_items = (
    enriched
    .withColumn("last_known_quantity_filled_bucket", F.last("quantity_filled_bucket", ignorenulls=True).over(w_running))
    .withColumn("first_known_order_id_sum_unit_price_int", F.first("order_id_sum_unit_price_int", ignorenulls=True).over(w6))
    .withColumn("order_id_min_prev_unit_price_int", F.min("prev_unit_price_int").over(w4))
)

metrics = (
    enriched_order_items
    .select(
        "*",
        F.sum("quantity_filled_bucket").over(w7).alias("cumulative_quantity_filled_bucket"),
        F.first("unit_price_filled", ignorenulls=True).over(w8).alias("first_known_unit_price_filled"),
        F.last("quantity_filled_cume_dist", ignorenulls=True).over(w9).alias("last_known_quantity_filled_cume_dist"),
        F.lag("unit_price_filled", 1).over(w10).alias("prev_unit_price_filled"),
        F.dense_rank().over(w11).alias("unit_price_plus_unit_price_int_dense_rank"),
    )
)

valid_order_items = (
    metrics
    .filter(F.col("prev_unit_price_filled") < 70)
    .select(
        "*",
        (F.col("unit_price_int_bucket") - F.col("first_known_unit_price_filled")).alias("unit_price_int_bucket_minus_first_known_unit_price_filled"),
    )
)

result = (
    valid_order_items
    .select(
        "id",
        "unit_price",
        "unit_price_int",
        "unit_price_plus_unit_price_int",
        "unit_price_filled",
        "prev_unit_price_int",
        "quantity_filled_cume_dist",
        "quantity_filled_bucket",
        "last_known_quantity_filled_bucket",
    )
)
