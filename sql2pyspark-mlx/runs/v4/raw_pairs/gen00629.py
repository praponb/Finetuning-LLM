from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity")
w2 = Window.partitionBy("order_id").orderBy("unit_price_filled", "id")
w_part = Window.orderBy("unit_price_filled", "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("unit_price_filled").desc(), "id")
w5 = Window.orderBy("unit_price_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("quantity_rank", "id")
w7 = Window.partitionBy("order_id").orderBy("unit_price_filled_bucket", "id")
w8 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("cumulative_prev_unit_price_filled", "id")
w10 = Window.partitionBy("order_id")
w11 = Window.partitionBy("order_id").orderBy(F.col("unit_price_filled").asc_nulls_first())
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.max("quantity").alias("max_order_item_quantity"))

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("product_id") == 8))
    .select(
        "id",
        "order_id",
        "quantity",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

enriched = (
    cleaned
    .withColumn("quantity_rank", F.rank().over(w))
    .withColumn("prev_unit_price_filled", F.lag("unit_price_filled", 2).over(w2))
    .withColumn("next_quantity", F.lead("quantity", 1).over(w_part))
    .withColumn("unit_price_filled_bucket", F.ntile(5).over(w4))
)

with_windows = (
    enriched
    .withColumn("cumulative_prev_unit_price_filled", F.sum("prev_unit_price_filled").over(w5))
    .withColumn("next_prev_unit_price_filled", F.lead("prev_unit_price_filled", 1, 0).over(w6))
)

enriched_order_items = (
    with_windows
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.shipping_fee"), F.col("b.amount"))
)

deduped = (
    enriched_order_items
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    deduped
    .select(
        "*",
        F.first("unit_price_filled", ignorenulls=True).over(w8).alias("first_known_unit_price_filled"),
        F.lead("next_quantity", 2).over(w9).alias("next_next_quantity"),
        F.stddev_samp("amount").over(w10).alias("order_id_stddev_samp_amount"),
        F.rank().over(w11).alias("unit_price_filled_rank"),
    )
)

result = (
    scored
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "unit_price_filled",
        "quantity_rank",
        "prev_unit_price_filled",
        "next_quantity",
        "unit_price_filled_bucket",
        "cumulative_prev_unit_price_filled",
        "next_prev_unit_price_filled",
        F.col("max_order_item_quantity"),
    )
)
