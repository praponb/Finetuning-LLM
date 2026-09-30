from pyspark.sql import functions as F

enriched = (
    df_inventory
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.last_restock",
        F.col("p.product_name"),
        F.col("p.stock_qty"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("QUARTER", "last_restock").alias("quarter_start"), "store_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles_2"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
    .orderBy(F.col("avg_stock_qty").desc_nulls_first())
)
