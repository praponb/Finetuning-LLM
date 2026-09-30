from pyspark.sql import functions as F

result = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.city").alias("city"), F.col("src.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("src.price")).alias("total_price"),
    )
)
