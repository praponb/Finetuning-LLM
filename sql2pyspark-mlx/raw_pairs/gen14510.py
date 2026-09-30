from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("initech_rows").desc())

with_dims = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.discount"), F.col("p.shipping_fee"), F.col("p.order_date"))
)

with_dims_order_items = (
    with_dims
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.brand"))
)

with_dims_order_items_2 = (
    with_dims_order_items
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .select(
        "e.*",
        F.col("b.discount").alias("order_discount"),
        F.col("b.amount"),
        F.col("b.channel"),
    )
)

matched_keys = (
    with_dims_order_items_2
    .select("order_id", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("order_id").isin([3, 5]))
        .select("order_id", "product_id")
    )
)

reconciled = (
    with_dims_order_items_2
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("WEEK", "order_date").alias("week_start"),
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_rows"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("quantity_per_store"),
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("quantity_per_acme"),
    )
    .withColumn("initech_rows_dense_rank", F.dense_rank().over(w))
)
