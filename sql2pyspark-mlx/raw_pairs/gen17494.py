from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("amount").desc_nulls_first())
w_recent = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("store_id").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

cleaned = (
    df_orders
    .filter((F.col("channel").eqNullSafe(F.lit("partner"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") <= F.col("s.order_date")), "left_anti")
)

latest = (
    not_flagged
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "rn",
        F.count("amount").over(w_recent).alias("last3_count_amount"),
        F.avg("amount").over(w_recent).alias("last3_avg_amount"),
        F.sum("amount").over(w_recent).alias("last3_sum_amount"),
        F.min("amount").over(w_recent).alias("last3_min_amount"),
        F.last("channel", ignorenulls=True).over(w3).alias("last_known_channel"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w4))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price"),
    )
)

with_windows = (
    latest_match
    .select(
        "id",
        "store_id",
        "amount",
        "rn",
        "last3_avg_amount",
        "last3_sum_amount",
        "last3_min_amount",
        "order_item_quantity",
        "unit_price",
        F.max("last3_sum_amount").over(w_part).alias("running_max_last3_sum_amount"),
        F.min("last3_sum_amount").over(w_part).alias("running_min_last3_sum_amount"),
        F.count("last3_sum_amount").over(w_part).alias("running_count_last3_sum_amount"),
    )
)

result = (
    with_windows
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "rn",
        "last3_avg_amount",
        "last3_min_amount",
        "order_item_quantity",
        "unit_price",
        "running_max_last3_sum_amount",
        "running_count_last3_sum_amount",
        F.col("avg_inventory_quantity"),
    )
    .orderBy(F.col("order_item_quantity").asc_nulls_first())
)
