from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("ss.units"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
    )
    .orderBy("region")
)
