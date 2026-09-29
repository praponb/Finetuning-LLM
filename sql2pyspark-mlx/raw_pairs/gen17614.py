from pyspark.sql import functions as F

result = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.country").alias("country"), F.col("a.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.price")).alias("min_price"),
    )
    .orderBy(F.col("category").asc_nulls_first())
)
