from pyspark.sql import functions as F

rollup_totals = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("dim"), F.col("a.supplier_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.country").alias("country"),
        F.col("a.category").alias("category"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.stock_qty")).alias("min_stock_qty"),
        F.round(F.sum(F.col("a.price")), 2).alias("total_price"),
    )
)

result = (
    rollup_totals
    .select("cnt", "total_price")
    .orderBy("total_price", "cnt")
)
