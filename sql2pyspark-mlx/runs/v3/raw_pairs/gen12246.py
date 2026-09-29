from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
        F.count("*").alias("num_store_sales"),
    )
)
