from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("last3_sum_quantity", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("product_id").orderBy("unit_price_minus_amount")
w4 = Window.partitionBy("product_id").orderBy("amount", "id")
w5 = Window.partitionBy("cost_tier").orderBy(F.col("num_rows").desc())

with_dims = (
    df_order_items
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select("src.*", F.col("o.order_date"), F.col("o.channel"), F.col("o.amount"))
)

base = (
    with_dims
    .filter(F.col("order_date") > "2025-05-01")
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "amount",
        F.floor("unit_price").alias("unit_price_whole"),
        (F.col("unit_price") - F.col("amount")).alias("unit_price_minus_amount"),
    )
)

with_windows = (
    base
    .filter(F.col("order_id").isin([7, 9, 25, 34]))
    .select(
        "*",
        F.count("quantity").over(w).alias("last3_count_quantity"),
        F.min("quantity").over(w).alias("last3_min_quantity"),
        F.avg("quantity").over(w).alias("last3_avg_quantity"),
        F.sum("quantity").over(w).alias("last3_sum_quantity"),
    )
)

joined = (
    with_windows
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.quantity",
        "a.unit_price",
        "a.amount",
        "a.unit_price_minus_amount",
        "a.last3_count_quantity",
        "a.last3_sum_quantity",
        F.col("d.stock_qty"),
        F.col("d.cost"),
    )
)

enriched = (
    joined
    .withColumn("quantity_avg_3_prior", F.avg("quantity").over(w2))
    .withColumn("unit_price_minus_amount_cume_dist", F.cume_dist().over(w3))
    .withColumn("next_unit_price_minus_amount", F.lead("unit_price_minus_amount", 1).over(w4))
)

grouped = (
    enriched
    .groupBy(
        F.when(F.col("cost") >= 670, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
        F.when(F.col("last3_count_quantity") >= 16, "HIGH").otherwise("LOW").alias("last3_count_quantity_tier"),
    )
    .agg(
        F.percentile_approx("stock_qty", 0.95).alias("stock_qty_p95"),
        F.count("*").alias("num_rows"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
    .withColumn("num_rows_rank", F.rank().over(w5))
)

result = (
    grouped
    .select(
        "cost_tier",
        "last3_count_quantity_tier",
        "quantity_bit_and",
        "num_rows_rank",
    )
    .orderBy(F.col("last3_count_quantity_tier").desc(), "quantity_bit_and")
)
