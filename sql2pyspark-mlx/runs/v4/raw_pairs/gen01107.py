from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("s.opened_date") > "2024-08-01")
    .select(
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("ss.store_id"),
    )
)
