from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .filter(F.col("S.revenue_range") > 61)
    .select(F.col("s.sqft"), F.col("s.region"), F.col("S.revenue_range"))
    .orderBy(F.col("sqft").desc())
)
