from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.region") != "North")
    .select(
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
    )
    .orderBy("store_name")
)
