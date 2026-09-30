from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity_filled", "id")
w3 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w4 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("unit_price").desc())
w6 = Window.partitionBy("product_id").orderBy("adjusted_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy(F.col("event_seq").desc())
w9 = Window.partitionBy("product_id").orderBy("last_known_quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("unit_price_rank", "id")
w11 = Window.partitionBy("product_id")

base = (
    df_order_items
    .filter(F.col("order_id").isin([16, 28, 35]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.round((F.col("unit_price") * 0.9), 2).alias("adjusted_unit_price"),
    )
)

scored = (
    base
    .withColumn("cumulative_unit_price", F.sum("unit_price").over(w_part))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w_part))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("unit_price_bucket", F.ntile(3).over(w3))
)

metrics = (
    scored
    .select(
        "*",
        F.last("quantity_filled", ignorenulls=True).over(w4).alias("last_known_quantity_filled"),
        F.rank().over(w5).alias("unit_price_rank"),
        F.last("unit_price", ignorenulls=True).over(w6).alias("last_known_unit_price"),
    )
)

base_order_items = (
    metrics
    .filter(F.col("unit_price_bucket") >= 4)
    .select(
        "id",
        "product_id",
        "quantity",
        "quantity_filled",
        "cumulative_unit_price",
        "event_seq",
        "unit_price_bucket",
        "last_known_quantity_filled",
        "unit_price_rank",
        "last_known_unit_price",
        F.round("event_seq", 2).alias("event_seq_rounded"),
    )
)

enriched = (
    base_order_items
    .withColumn("first_known_event_seq", F.first("event_seq", ignorenulls=True).over(w7))
    .withColumn("event_seq_percent_rank", F.percent_rank().over(w8))
    .withColumn("running_last_known_quantity_filled", F.sum("last_known_quantity_filled").over(w9))
    .withColumn("unit_price_rank_bucket", F.ntile(5).over(w10))
    .withColumn("unit_price_rank_share", F.round((F.col("unit_price_rank") / F.sum("unit_price_rank").over(w11)), 4))
)

result = (
    enriched
    .select(
        "id",
        "quantity",
        "cumulative_unit_price",
        "event_seq",
        "last_known_unit_price",
        "event_seq_rounded",
        "first_known_event_seq",
        "event_seq_percent_rank",
        "running_last_known_quantity_filled",
    )
    .orderBy("event_seq")
)
