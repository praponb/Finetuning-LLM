from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id", "product_id")
w3 = Window.partitionBy("order_id").orderBy("prev_quantity", "id")

metrics = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.lead("unit_price").over(w_running).alias("next_unit_price"),
        F.lag("quantity").over(w_running).alias("prev_quantity"),
        F.lead("quantity").over(w_running).alias("next_quantity"),
        F.row_number().over(w_running).alias("row_seq"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4).alias("unit_price_share"),
    )
)

with_windows = (
    metrics
    .filter(F.col("next_quantity") <= 8)
    .select(
        "*",
        F.lead("next_quantity").over(w3).alias("next_next_quantity"),
        F.lag("next_quantity").over(w3).alias("prev_next_quantity"),
        F.lag("quantity").over(w3).alias("prev_quantity_2"),
        F.row_number().over(w3).alias("row_seq_2"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("next_unit_price") >= 360)
    .select(
        "*",
        (F.col("row_seq") / F.nullif("unit_price_share", F.lit(0))).alias("row_seq_per_unit_price_share"),
        F.floor("row_seq_2").alias("row_seq_2_whole"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.discount"), F.col("b.channel"), F.col("b.amount"))
)

filtered = (
    with_dims
    .filter(F.col("channel").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("mobile"))))
    .select(
        "*",
        F.greatest("row_seq", "prev_next_quantity").alias("greatest_row_seq_prev_next_quantity"),
        F.floor("unit_price_share").alias("unit_price_share_whole"),
    )
)

base = (
    filtered
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.least("row_seq_2", "unit_price_share").alias("least_row_seq_2_unit_price_share"),
        F.round((F.col("row_seq_2_whole") * 1.07), 2).alias("row_seq_2_whole_scaled"),
    )
)

base_order_items = (
    base
    .filter(F.col("next_quantity") > 4)
    .withColumn("channel_filled", F.coalesce("channel", F.lit("N/A")))
)

result = (
    base_order_items
    .groupBy("order_id")
    .pivot("channel", ["partner", "store"])
    .agg(F.avg("amount").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
