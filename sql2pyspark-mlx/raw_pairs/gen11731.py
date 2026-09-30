from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.sale_date") < "2024-02-01")
    .select(
        F.col("s.city"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
        F.col("ss.store_id"),
    )
    .orderBy(F.col("units").desc())
)
