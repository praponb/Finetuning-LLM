from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("sale_date").between("2023-12-01", "2025-06-17")).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_anti")
    .filter(F.length(F.col("s.store_name")) > 13)
    .select(F.col("s.id"), F.col("s.sqft"))
)
