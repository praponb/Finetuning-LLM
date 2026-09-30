from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date") <= "2025-05-11")
    .select(
        F.col("ss.units"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
)
