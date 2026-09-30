from pyspark.sql import functions as F

by_dimension = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "inner")
    .groupingSets(
        [["city", "brand"], ["city"], []],
        F.col("s.city").alias("city"),
        F.col("a.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("a.cost")), 2).alias("total_cost"),
        F.avg(F.col("a.stock_qty")).alias("avg_stock_qty"),
        F.grouping_id().alias("gid"),
    )
)

result = by_dimension.select("city", "brand", "total_cost")
