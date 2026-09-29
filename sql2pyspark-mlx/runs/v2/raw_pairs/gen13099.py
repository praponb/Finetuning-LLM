from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.month("opened_date").isin([7, 9, 10])).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("ss.revenue").between(2200, 2300))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
    )
    .orderBy("store_id", "id")
    .limit(5)
)
