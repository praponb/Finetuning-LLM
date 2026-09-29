from pyspark.sql import functions as F

result = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "inner")
    .groupBy(
        F.col("d.country").alias("country"),
        F.col("d.city").alias("city"),
        F.col("e.category").alias("category"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("e.cost")).alias("avg_cost"),
        F.max(F.col("e.stock_qty")).alias("max_stock_qty"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
