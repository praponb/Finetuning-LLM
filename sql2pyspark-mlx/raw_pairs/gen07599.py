from pyspark.sql import functions as F

result = (
    df_products
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.country").alias("country"), F.col("d.city").alias("city"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
        F.round(F.sum(F.col("s.stock_qty")), 2).alias("total_stock_qty"),
        F.grouping_id().alias("gid"),
    )
)
