from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.count("*").alias("num_store_sales"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("ss.avg_units"),
        F.col("ss.num_store_sales"),
    )
    .orderBy(F.col("sqft").desc())
)
