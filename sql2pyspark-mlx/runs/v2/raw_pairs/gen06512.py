from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("store_id") == 6)
    .groupBy("store_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("S.latest_sale_date"),
    )
)
