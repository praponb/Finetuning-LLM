from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id", "brand").orderBy(F.col("dedup_rank").desc(), "id")

deduped = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    deduped
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.brand"))
)

ranked = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    ranked
    .filter(F.col("brand").isNotNull())
    .select(
        "*",
        F.round("dedup_rank", 2).alias("dedup_rank_rounded"),
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

base = (
    valid
    .filter(F.col("quantity").between(6, 7))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "dedup_rank",
        "brand",
        F.least("dedup_rank_rounded", "unit_price").alias("least_dedup_rank_rounded_unit_price"),
    )
)

with_dims_order_items = (
    base
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.category") == "Grocery")
    .select("src.*", F.col("b.price"), F.col("b.category"))
)

joined = (
    with_dims_order_items
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.quantity",
        "src.unit_price",
        "src.dedup_rank",
        "src.price",
        F.col("d.discount"),
        F.col("d.amount"),
        F.col("d.status"),
    )
)

result = (
    joined
    .withColumn("price_tier", F.when(F.col("price") >= 960, "PREMIUM").otherwise("STANDARD"))
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 360, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["price_tier", "unit_price_tier", "product_id"], ["price_tier", "unit_price_tier"], ["price_tier"]],
        "price_tier",
        "unit_price_tier",
        "product_id",
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("discount")).otherwise(0)).alias("pending_discount"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
