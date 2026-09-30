from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.sale_date").between("2023-06-23", "2025-02-21"))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
        F.col("ss.store_id"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.date_format(F.col("ss.sale_date"), "yyyy-MM-dd").alias("sale_date_label"),
    )
)
