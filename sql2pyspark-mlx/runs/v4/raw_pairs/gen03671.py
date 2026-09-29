from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.sum("units").alias("total_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("t.revenue_range"),
        F.col("t.total_units"),
    )
    .orderBy("id")
)
