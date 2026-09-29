from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(F.min(F.col("p.launch_date")).alias("first_launch_date"))
    .orderBy("first_launch_date")
)
