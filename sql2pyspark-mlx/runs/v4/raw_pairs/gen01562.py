from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(~F.col("region").isin(["West", "South", "North"])).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
    )
)
