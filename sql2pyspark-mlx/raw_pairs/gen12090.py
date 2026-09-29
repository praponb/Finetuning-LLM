from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("unit_price").desc())
w3 = Window.partitionBy("order_id").orderBy(F.col("running_avg_unit_price").desc())

metrics = (
    df_order_items
    .withColumn("running_avg_unit_price", F.avg("unit_price").over(w))
    .withColumn("running_count_unit_price", F.count("unit_price").over(w))
    .withColumn("running_sum_unit_price", F.sum("unit_price").over(w))
    .withColumn("unit_price_dense_rank", F.dense_rank().over(w2))
)

remaining_keys = (
    metrics
    .select("id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id") == 13)
        .select("id", "product_id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee").isNull())
    .select(
        "a.id",
        "a.order_id",
        "a.unit_price",
        "a.running_avg_unit_price",
        "a.running_count_unit_price",
        "a.running_sum_unit_price",
        "a.unit_price_dense_rank",
        F.col("o.status"),
    )
)

kept_keys = (
    enriched
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id") == 35)
        .select("id", "order_id")
    )
)

kept_rows_order_items = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows_order_items
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.shipping_fee"))
)

scored = with_dims.withColumn("running_avg_unit_price_rank", F.rank().over(w3))

daily = (
    scored
    .groupBy(
        F.when(F.col("shipping_fee") >= 38, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.percentile_approx("running_avg_unit_price", 0.95).alias("running_avg_unit_price_p95"),
        F.max("running_avg_unit_price").alias("max_running_avg_unit_price"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
    )
)

result = (
    daily
    .select("shipping_fee_tier", "running_avg_unit_price_p95", "avg_shipping_fee")
    .orderBy(F.col("avg_shipping_fee").desc())
)
