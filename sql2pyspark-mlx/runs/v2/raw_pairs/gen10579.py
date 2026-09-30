from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("region") == "Central").alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("ss.revenue") < 3800)
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
    )
)
