from pyspark.sql import functions as F

base = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-07-24").cast("date")) & (F.col("quantity") == 99))
    .select("*", F.date_sub("last_restock", 14).alias("last_restock_minus_14d"))
)

kept_keys = (
    base
    .select("id", "store_id")
    .intersect(
        df_inventory
        .filter(F.col("last_restock") >= "2025-09-01")
        .select("id", "store_id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("store_id").isin([1, 2, 5, 6]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "last_restock_minus_14d",
        F.date_add("last_restock", 42).alias("last_restock_plus_42d"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.product_name"), F.col("d.stock_qty"))
)

daily = (
    enriched
    .groupBy(F.date_trunc("week", "last_restock_plus_42d").alias("week_start"))
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    daily
    .select("week_start", "quantity_stddev", "distinct_ids")
    .orderBy("distinct_ids", "week_start")
)
