from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.col("s.city"))
    .agg(F.max(F.col("p.launch_date")).alias("last_launch_date"))
    .orderBy("city", "last_launch_date")
)
