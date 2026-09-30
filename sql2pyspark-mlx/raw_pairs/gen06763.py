from pyspark.sql import functions as F

by_dimension = (
    df_store_sales
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city").alias("city"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("e.revenue")).alias("avg_revenue"),
    )
)

result = (
    by_dimension
    .select("city", "cnt")
    .orderBy(F.col("cnt").desc())
)
