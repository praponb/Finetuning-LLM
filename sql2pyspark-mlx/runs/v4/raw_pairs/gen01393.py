from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

base = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2024-04-06").cast("date")) & F.col("customer_id").isNotNull() & (F.col("discount") > 0.38))
    .select(
        "*",
        F.col("discount").cast("int").alias("discount_int"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

base_orders = (
    base
    .filter(F.col("order_date") > F.lit("2023-09-19").cast("date"))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "shipping_fee",
        "discount_int",
        "channel_filled",
        F.least("amount", "discount").alias("least_amount_discount"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled_2"),
    )
)

ranked = (
    base_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

stats = (
    ranked
    .groupBy("store_id", "channel")
    .agg(
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_rows"),
        F.count("*").alias("event_count"),
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.avg(F.when(F.col("channel") == "partner", F.col("shipping_fee"))).alias("partner_avg_shipping_fee"),
    )
    .filter(F.col("shipping_fee_stddev") < 0)
)

result = (
    stats
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "channel",
        "pending_rows",
        "event_count",
        "avg_shipping_fee",
        F.when(F.col("channel").rlike("^(mobile|store)$"), "PRIORITY").when(F.col("channel").rlike("^(web|partner)$"), "GROUP_A").otherwise("GROUP_B").alias("channel_zone"),
        F.col("sum_inventory_quantity"),
    )
    .orderBy(F.col("store_id").desc_nulls_first())
)
