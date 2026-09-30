from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("product_id").orderBy("unit_price_bucket", "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.sum("quantity").over(w_ordered).alias("running_sum_quantity"),
        F.avg("quantity").over(w_ordered).alias("running_avg_quantity"),
        F.min("quantity").over(w_ordered).alias("running_min_quantity"),
        F.ntile(10).over(w2).alias("unit_price_bucket"),
    )
)

enriched_order_items = (
    enriched
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "running_avg_quantity",
        "unit_price_bucket",
        F.round((F.col("unit_price_bucket") / F.sum("unit_price_bucket").over(w3)), 4).alias("unit_price_bucket_share"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

joined = (
    enriched_order_items
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.shipping_fee"), F.col("p.discount"), F.col("p.amount"))
)

kept_keys = (
    joined
    .select("order_id", "id")
    .intersect(
        df_order_items
        .filter(F.col("unit_price") > 190)
        .select("order_id", "id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.avg("unit_price_bucket_share").alias("avg_unit_price_bucket_share"),
        F.percentile_approx("unit_price_bucket", 0.5).alias("unit_price_bucket_p50"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
    )
)
