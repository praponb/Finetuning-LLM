from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .filter(F.col("sale_date") <= "2025-10-15")
    .groupBy("store_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .filter(F.col("S.avg_revenue") >= 1300)
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.city"), F.col("S.avg_revenue"))
    .orderBy(F.col("id").desc())
)
