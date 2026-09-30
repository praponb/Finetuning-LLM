from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("store_id") == 2).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .filter(F.col("s.opened_date").between("2023-05-01", "2025-10-26"))
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.opened_date"), F.col("s.city"))
)
