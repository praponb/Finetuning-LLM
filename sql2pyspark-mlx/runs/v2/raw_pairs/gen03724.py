from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("city").isin(["Bangkok", "New York"])).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
    )
    .orderBy("units", "id")
    .limit(1)
)
