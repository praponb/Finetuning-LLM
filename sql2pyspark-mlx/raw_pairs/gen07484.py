from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(F.max(F.col("p.launch_date")).alias("latest_launch_date"))
    .orderBy("city")
    .limit(20)
)
