from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("ss.revenue"),
        F.col("ss.store_id"),
    )
    .orderBy(F.col("store_id").desc())
)
