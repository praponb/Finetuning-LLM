from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("city") == "Singapore").alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("ss.revenue").isNull())
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("ss.store_id"),
    )
    .orderBy("id")
)
