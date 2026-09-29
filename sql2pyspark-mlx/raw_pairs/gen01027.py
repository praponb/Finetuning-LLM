from pyspark.sql import functions as F

kept_keys = (
    df_products
    .select("category")
    .intersectAll(
        df_products
        .filter(F.col("cost") > 510)
        .select("category")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.when(F.col("cost") >= 650, "HIGH").otherwise("LOW").alias("cost_tier"))
    .agg(
        F.percentile_approx("price", 0.95).alias("price_p95"),
        F.countDistinct("category").alias("distinct_categories"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
    )
    .orderBy(F.col("price_p95").asc_nulls_last())
)
