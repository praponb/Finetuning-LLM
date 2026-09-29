from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
    )
    .orderBy("opened_date", "id")
    .limit(5)
)
