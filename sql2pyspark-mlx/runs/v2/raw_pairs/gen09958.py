from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"), F.col("s.country"))
    .agg(F.max(F.col("p.launch_date")).alias("latest_launch_date"))
)
