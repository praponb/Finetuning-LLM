from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id")
w_part = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("unit_price") <= 200)
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    ranked
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.line_total",
        F.col("d.amount"),
        F.col("d.discount"),
        F.col("d.status"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.line_total",
        "src.amount",
        F.col("p.discount").alias("order_discount"),
        F.col("p.shipping_fee"),
    )
)

scored = (
    joined
    .withColumn("last4_count_order_discount", F.count("order_discount").over(w_part))
    .withColumn("last4_sum_order_discount", F.sum("order_discount").over(w_part))
    .withColumn("last4_min_order_discount", F.min("order_discount").over(w_part))
    .withColumn("last4_avg_order_discount", F.avg("order_discount").over(w_part))
)

remaining_keys = (
    scored
    .select("product_id", "order_id")
    .exceptAll(
        df_order_items
        .filter(F.col("unit_price") > 240)
        .select("product_id", "order_id")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.product_name"), F.col("p.launch_date"), F.col("p.price"))
)

result = (
    with_dims
    .rollup(
        "product_id",
        F.when(F.col("order_discount") >= 0.38, "HIGH").otherwise("LOW").alias("order_discount_tier"),
    )
    .agg(
        F.percentile_approx("order_discount", 0.9).alias("order_discount_p90"),
        F.round(F.avg("last4_avg_order_discount"), 2).alias("avg_last4_avg_order_discount"),
    )
)
