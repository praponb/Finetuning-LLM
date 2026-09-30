from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.month("sale_date").isin([3, 7, 12])).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.region"))
    .orderBy("region", "id")
    .limit(10)
)
