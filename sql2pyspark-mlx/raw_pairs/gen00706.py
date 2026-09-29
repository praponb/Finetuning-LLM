from pyspark.sql import functions as F

by_dimension = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["city", "brand"], ["city"], []],
        F.col("p.city").alias("city"),
        F.col("e.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.stock_qty")).alias("max_stock_qty"),
    )
)

result = by_dimension.select("rows_total", "max_stock_qty")
