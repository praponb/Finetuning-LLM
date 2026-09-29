from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.sum("units").alias("total_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("t.avg_revenue"),
        F.col("t.total_units"),
    )
    .orderBy(F.col("sqft").desc())
)
