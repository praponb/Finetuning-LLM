from pyspark.sql import functions as F

rollup_totals = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "inner")
    .rollup(F.col("s.city").alias("city"), F.col("src.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.stock_qty")).alias("max_stock_qty"),
        F.min(F.col("src.cost")).alias("min_cost"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    rollup_totals
    .select(
        "category",
        "max_stock_qty",
        "min_cost",
        "level_id",
        F.when(F.col("category").rlike("^(Home|Books)$"), "PRIORITY").when(F.col("category").rlike("^(Electronics|Sports)$"), "GROUP_A").otherwise("CORE").alias("category_zone"),
    )
)
