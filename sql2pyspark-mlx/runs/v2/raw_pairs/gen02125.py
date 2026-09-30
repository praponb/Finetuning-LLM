from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.units") < 15)
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        (F.max(F.col("ss.revenue")) - F.min(F.col("ss.revenue"))).alias("revenue_range"),
        F.count(F.col("ss.units")).alias("units_count"),
    )
    .orderBy(F.col("id").desc())
)
