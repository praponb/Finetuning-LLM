from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("p.launch_date").between("2023-11-01", "2025-09-01"))
    .groupBy(F.col("p.supplier_id"))
    .agg(F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"))
    .orderBy("supplier_id", F.col("avg_units").desc())
    .limit(10)
)
