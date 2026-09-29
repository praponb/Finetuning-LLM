from pyspark.sql import functions as F

dim_totals = (
    df_store_sales
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .groupingSets(
        [["city", "region"], ["city"]],
        F.col("s.city").alias("city"),
        F.col("s.region").alias("region"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("e.revenue")).alias("avg_revenue"),
    )
)

result = (
    dim_totals
    .select("num_rows")
    .orderBy(F.col("num_rows").asc_nulls_last())
)
