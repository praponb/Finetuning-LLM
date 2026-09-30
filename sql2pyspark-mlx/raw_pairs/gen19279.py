from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.region") == "North") & F.month(F.col("s.opened_date")).isin([5, 6, 8]))
    .select(F.col("s.city"), F.col("s.region"), F.col("ss.revenue"), F.col("ss.units"))
)
