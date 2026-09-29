from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("sale_date").between("2023-08-01", "2025-02-04")).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .filter(F.upper(F.col("s.store_name")).like("%IVE%"))
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
    .orderBy("store_name")
)
