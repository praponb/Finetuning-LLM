from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")
w2 = Window.partitionBy("channel").orderBy(F.col("shipping_fee").asc_nulls_first(), "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("discount").desc_nulls_last(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("status").orderBy("order_item_quantity", "id")

valid = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-10-04").cast("date")) & F.col("store_id").isNotNull())
    .select(
        "*",
        F.ceil("discount").alias("discount_whole"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "resolved_channel",
    )
)

deduped_orders = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

ranked = (
    deduped_orders
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

with_last = (
    ranked
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") < F.col("s.order_date")), "left_anti")
)

enriched = (
    eligible
    .select(
        "id",
        "channel",
        "discount",
        "shipping_fee",
        "resolved_channel",
        "row_num",
        "order_item_unit_price",
        F.lead("order_item_unit_price").over(w5).alias("next_order_item_unit_price"),
        F.lag("order_item_unit_price").over(w5).alias("prev_order_item_unit_price"),
        F.lag("order_item_quantity").over(w5).alias("prev_order_item_quantity"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("row_seq") >= 7, "HIGH").otherwise("LOW").alias("row_seq_tier"),
    )
    .agg(
        F.round(F.sum("next_order_item_unit_price") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("next_order_item_unit_price_per_store"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 1000)
    .orderBy("next_order_item_unit_price_per_store")
)
