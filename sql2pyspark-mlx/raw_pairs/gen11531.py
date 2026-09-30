from pyspark.sql import functions as F

remaining_keys = (
    df_products
    .select("id", "category")
    .intersect(
        df_products
        .filter(F.col("product_name").like("%Mat%"))
        .select("id", "category")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("stock_qty") >= 334, "HIGH").otherwise("LOW").alias("stock_qty_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum("stock_qty").alias("total_stock_qty"),
        F.max("cost").alias("max_cost"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_rows"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("sports_rows").desc_nulls_last())
)
