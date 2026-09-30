from pyspark.sql import functions as F

result = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "left")
    .groupBy(
        F.col("d.country").alias("country"),
        F.col("d.city").alias("city"),
        F.col("src.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.stock_qty")).alias("max_stock_qty"),
    )
)
