from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("is_partner", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("is_partner").desc(), "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

joined = (
    valid
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.stock_qty") == 166)
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        F.col("d.brand"),
        F.col("d.launch_date"),
        F.col("d.stock_qty"),
    )
)

enriched = (
    joined
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.id", "e.order_id", "e.brand", F.col("p.channel"))
)

base = (
    enriched
    .filter(F.col("brand").isNotNull())
    .withColumn("is_partner", F.when(F.col("channel") == "partner", 1).otherwise(0))
)

with_windows = (
    base
    .select(
        "*",
        F.sum("is_partner").over(w_ordered).alias("cumulative_is_partner"),
        F.ntile(10).over(w2).alias("is_partner_bucket"),
    )
)

result = (
    with_windows
    .withColumn("cumulative_is_partner_tier", F.when(F.col("cumulative_is_partner") >= 399, "LARGE").otherwise("SMALL"))
    .withColumn("is_partner_tier", F.when(F.col("is_partner") >= 61, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["cumulative_is_partner_tier", "is_partner_tier", "channel"], ["cumulative_is_partner_tier", "is_partner_tier"], ["cumulative_is_partner_tier"]],
        "cumulative_is_partner_tier",
        "is_partner_tier",
        "channel",
    )
    .agg(
        F.percentile_approx("is_partner", 0.95).alias("is_partner_p95"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.round(F.avg("cumulative_is_partner"), 2).alias("avg_cumulative_is_partner"),
        F.count("*").alias("num_rows"),
        F.grouping("cumulative_is_partner_tier").alias("grouping_level"),
    )
    .orderBy(F.col("is_partner_tier").desc())
)
