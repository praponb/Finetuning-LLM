from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.month("order_date"), "status").orderBy("order_item_quantity", "id")
w3 = Window.orderBy("order_date", "id")
w4 = Window.partitionBy("customer_id", "channel").orderBy("discount_rounded", "id")
w5 = Window.orderBy(F.col("discount_times_amount").desc_nulls_first())
w6 = Window.partitionBy("channel").orderBy("discount_rounded", "id")

base = (
    df_orders
    .filter((F.col("channel").eqNullSafe(F.lit("web"))) & F.col("customer_id").isNull())
    .select(
        "*",
        (F.col("discount") * F.col("amount")).alias("discount_times_amount"),
        F.round("discount", 2).alias("discount_rounded"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("order_item_quantity"))
)

enriched = (
    latest_match
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "shipping_fee",
        "discount_times_amount",
        F.lag("discount", 1).over(w2).alias("prev_discount"),
        F.row_number().over(w3).alias("seq_no"),
        F.lag("order_item_quantity", 1).over(w4).alias("prev_order_item_quantity"),
        F.percent_rank().over(w5).alias("discount_times_amount_percent_rank"),
        F.lag("order_item_quantity", 1).over(w6).alias("prev_order_item_quantity_2"),
    )
)

base_orders = (
    enriched
    .filter(F.col("prev_order_item_quantity_2") != 7)
    .select(
        "*",
        (F.col("prev_order_item_quantity") + F.col("prev_discount")).alias("prev_order_item_quantity_plus_prev_discount"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

result = (
    base_orders
    .groupBy(
        "channel",
        F.when(F.col("seq_no") >= 7, "HIGH").otherwise("LOW").alias("seq_no_tier"),
    )
    .agg(
        F.round(F.sum("prev_order_item_quantity_plus_prev_discount") / F.nullif(F.count(F.when(F.col("status") == "SHIPPED", 1)), F.lit(0)), 2).alias("prev_order_item_quantity_plus_prev_discount_per_shipped"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("discount_times_amount_percent_rank"))).alias("completed_total_discount_times_amount_percent_rank"),
    )
)
