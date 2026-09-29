from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")
w_hist = Window.orderBy("prev_quantity", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.partitionBy("order_id", "product_id").orderBy("adjusted_unit_price", "id")
w4 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy(F.col("adjusted_unit_price").desc(), "id")
w6 = Window.orderBy("unit_price_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy("adjusted_unit_price_avg_3", "id")
w_part = Window.orderBy("unit_price_adjusted_adjusted", "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", F.round((F.col("unit_price") * 0.85), 2).alias("unit_price_adjusted"))
)

cleaned = (
    valid
    .filter(F.col("product_id").isNotNull())
    .withColumn("adjusted_unit_price", F.round((F.col("unit_price") * 1.07), 2))
)

enriched = (
    cleaned
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price_adjusted",
        "adjusted_unit_price",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.lead("unit_price_adjusted").over(w).alias("next_unit_price_adjusted"),
        F.row_number().over(w).alias("seq_no"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price_adjusted",
        "adjusted_unit_price",
        F.avg("adjusted_unit_price").over(w_hist).alias("adjusted_unit_price_avg_3"),
        F.row_number().over(w_running).alias("seq_no_2"),
    )
)

filtered = (
    scored
    .filter(F.col("order_id") == 22)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price_adjusted",
        "adjusted_unit_price",
        "adjusted_unit_price_avg_3",
        "seq_no_2",
        F.round((F.col("unit_price_adjusted") * 100), 2).alias("unit_price_adjusted_adjusted"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "adjusted_unit_price_avg_3",
        "seq_no_2",
        "unit_price_adjusted_adjusted",
        F.sum("adjusted_unit_price").over(w4).alias("cumulative_adjusted_unit_price"),
        F.row_number().over(w5).alias("event_seq"),
        F.sum("quantity").over(w6).alias("cumulative_quantity"),
        F.lead("unit_price_adjusted_adjusted", 1).over(w7).alias("next_unit_price_adjusted_adjusted"),
    )
)

scored_order_items = (
    with_windows
    .select(
        "*",
        F.lead("adjusted_unit_price_avg_3").over(w_part).alias("next_adjusted_unit_price_avg_3"),
        F.lag("adjusted_unit_price_avg_3").over(w_part).alias("prev_adjusted_unit_price_avg_3"),
        F.lead("cumulative_quantity").over(w_part).alias("next_cumulative_quantity"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

result = (
    scored_order_items
    .select(
        "id",
        "adjusted_unit_price_avg_3",
        "seq_no_2",
        "unit_price_adjusted_adjusted",
        "cumulative_adjusted_unit_price",
        "event_seq",
        "cumulative_quantity",
        "next_unit_price_adjusted_adjusted",
        "next_adjusted_unit_price_avg_3",
    )
    .orderBy(F.col("cumulative_adjusted_unit_price").desc_nulls_last())
)
