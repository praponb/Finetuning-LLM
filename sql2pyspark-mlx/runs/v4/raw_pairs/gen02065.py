from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

cleaned = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-09-10").cast("date")) & F.col("brand").isNotNull())
    .withColumn("launch_date_plus_months", F.add_months("launch_date", 6))
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.order_id").isin([34, 40])), "left_semi")
)

kept_products = (
    kept
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") >= F.col("s.launch_date_plus_months")), "left_semi")
)

enriched = (
    kept_products
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.city"))
)

summary = (
    enriched
    .groupBy(
        F.when(F.col("stock_qty") >= 366, "LARGE").otherwise("SMALL").alias("stock_qty_tier"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Sports", F.col("cost")).otherwise(0)).alias("sports_cost"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.sum(F.when(F.col("city") == "London", F.col("stock_qty")).otherwise(0)).alias("london_stock_qty"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    summary
    .select("stock_qty_tier", "sports_cost")
    .orderBy(F.col("sports_cost").desc_nulls_last())
)
