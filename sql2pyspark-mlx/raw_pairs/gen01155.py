from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("order_id")
w3 = Window.orderBy(F.col("unit_price_whole").desc(), "id")
w4 = Window.orderBy("order_id_count_line_total", "id")
w5 = Window.orderBy("last_known_line_total", "id")
w6 = Window.orderBy(F.col("order_id_min_unit_price").asc_nulls_first())

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "unit_price",
        F.floor("unit_price").alias("unit_price_whole"),
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "unit_price",
        "unit_price_whole",
        "line_total",
        F.last("line_total", ignorenulls=True).over(w_hist).alias("last_known_line_total"),
        F.count("line_total").over(w_part).alias("order_id_count_line_total"),
        F.min("unit_price").over(w_part).alias("order_id_min_unit_price"),
    )
)

scored = (
    with_windows
    .withColumn("unit_price_whole_bucket", F.ntile(4).over(w3))
    .withColumn("next_order_id_min_unit_price", F.lead("order_id_min_unit_price", 1).over(w4))
    .withColumn("next_order_id_min_unit_price_2", F.lead("order_id_min_unit_price", 2).over(w5))
    .withColumn("order_id_min_unit_price_dense_rank", F.dense_rank().over(w6))
)

valid_order_items = (
    scored
    .filter(F.col("last_known_line_total") < 54)
    .select(
        "id",
        "unit_price",
        "unit_price_whole",
        "line_total",
        "last_known_line_total",
        "order_id_min_unit_price",
        "unit_price_whole_bucket",
        "next_order_id_min_unit_price",
        "next_order_id_min_unit_price_2",
        "order_id_min_unit_price_dense_rank",
        (F.col("order_id_count_line_total") - F.col("order_id_min_unit_price")).alias("order_id_count_line_total_minus_order_id_min_unit_price"),
    )
)

valid_order_items_2 = (
    valid_order_items
    .filter(F.col("unit_price_whole_bucket") <= 6)
    .select(
        "id",
        "unit_price",
        "line_total",
        "unit_price_whole_bucket",
        "next_order_id_min_unit_price",
        "next_order_id_min_unit_price_2",
        "order_id_min_unit_price_dense_rank",
        "order_id_count_line_total_minus_order_id_min_unit_price",
        F.greatest("order_id_min_unit_price", "unit_price_whole").alias("greatest_order_id_min_unit_price_unit_price_whole"),
    )
)

result = (
    valid_order_items_2
    .select(
        "id",
        "line_total",
        "unit_price_whole_bucket",
        "next_order_id_min_unit_price",
        "next_order_id_min_unit_price_2",
        "order_id_count_line_total_minus_order_id_min_unit_price",
        "greatest_order_id_min_unit_price_unit_price_whole",
    )
    .orderBy(F.col("id").asc_nulls_first())
)
