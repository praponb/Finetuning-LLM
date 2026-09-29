from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.cost").between(330, 670))
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.round(((100 * F.sum(F.when(F.col("p.category") == "Electronics", 1).otherwise(0))) / F.count("*")), 1).alias("pct_electronics"),
        F.min(F.col("p.stock_qty")).alias("min_stock_qty"),
    )
    .filter(F.col("product_count") > 1)
    .orderBy(F.col("min_stock_qty").desc())
)
